import os
import sys
from shlex import quote as shq
import time
import logging
import asyncio
import random
import json
import aiohttp
import base64
import uuid
import shutil
from aiohttp import web
import concurrent
import aiodocker
from aiodocker.exceptions import DockerError
from hailtop.utils import request_retry_transient_errors

# import uvloop

from hailtop.config import DeployConfig
from gear import configure_logging

from .utils import parse_cpu_in_mcpu, parse_image_tag
from .semaphore import NullWeightedSemaphore, WeightedSemaphore
from .log_store import LogStore
from .google_storage import GCS

# uvloop.install()

configure_logging()
log = logging.getLogger('batch2-agent')

docker = aiodocker.Docker()

MAX_IDLE_TIME_WITH_PODS = 60 * 2  # seconds
MAX_IDLE_TIME_WITHOUT_PODS = 60 * 1  # seconds


async def docker_call_retry(f, *args, **kwargs):
    delay = 0.1
    while True:
        try:
            return f(*args, **kwargs)
        except DockerError as e:
            # 408 request timeout, 503 service unavailable
            if e.status == 408 or e.status == 503:
                pass
            else:
                raise
        except asyncio.TimeoutError:
            pass
        # exponentially back off, up to (expected) max of 30s
        t = delay * random.random()
        await asyncio.sleep(t)
        delay = min(delay * 2, 60.0)


class Container:
    def __init__(self, spec, pod, log_directory):
        self.pod = pod
        self._container = None
        self.name = spec['name']
        self.spec = spec
        self.cores_mcpu = parse_cpu_in_mcpu(spec['cpu'])
        self.exit_code = None
        self.id = pod.name + '-' + self.name
        self.log_directory = log_directory

        tag = parse_image_tag(self.spec['image'])
        if not tag:
            log.info(f'adding latest tag to image {self.spec["image"]} for container {self.id}')
            self.spec['image'] += ':latest'

    async def create(self):
        log.info(f'creating container {self.id}')

        image = self.spec['image']
        config = {
            "AttachStdin": False,
            "AttachStdout": False,
            "AttachStderr": False,
            "Tty": False,
            'OpenStdin': False,
            'Cmd': self.spec['command'],
            'Image': image,
            'HostConfig': {'CpuPeriod': 100000,
                           'CpuQuota': self.cores_mcpu * 100}
        }

        volume_mounts = self.spec.get('volume_mounts')
        if volume_mounts:
            config['HostConfig']['Binds'] = volume_mounts

        # make sure we have container
        try:
            await docker_call_retry(docker.images.get, image)
        except DockerError as e:
            if e.status == 404:
                await docker_call_retry(docker.images.pull, image)

        self._container = await docker.containers.create(config)

    async def run(self):
        log.info(f'running container {self.id}')

        await docker_call_retry(self._container.start)
        await docker_call_retry(await self._container.wait)

        cinfo = await docker_call_retry(self._container.show)
        self.exit_code = self._container['State']['ExitCode']

        log_path = LogStore.container_log_path(self.log_directory, self.name)
        log_data = await self.log()
        upload_log = self.pod.worker.gcs_client.write_gs_file(log_path, log_data)

        status_path = LogStore.container_status_path(self.log_directory, self.name)
        status_data = json.dumps(cinfo, indent=4)
        upload_status = self.pod.worker.gcs_client.write_gs_file(status_path, status_data)

        await asyncio.gather(upload_log, upload_status)

        log.info(f'wrote log to {log_path}, status to {status_path} for container {self.id}')

    async def delete(self):
        if self._container is not None:
            await docker_call_retry(self._container.stop)
            await docker_call_retry(self._container.delete)
            self._container = None

    async def log(self):
        logs = await docker_call_retry(self._container.log, stderr=True, stdout=True)
        return "".join(logs)

    def to_dict(self):
        if self._container is None:
            return {
                'state': 'pending'
            }

        cinfo = await docker_call_retry(self.container.show)
        log.info(f'container info {cinfo}')

        cstate = cinfo['State']
        return {
            'state': cstate['Status'],
            'exit_code': cstate['ExitCode'],
            'error': cstate['Error'],
            'started_at': cstate['StartedAt'],
            'finished_at:' cstate['FinishedAt']
        }


class Volume:
    def __init__(self, name):
        self.name = name
        self.volume = None

    async def create(self):
        config = {
            'Name': self.name
        }
        self.volume = await docker.volumes.create(config)

    @property
    def volume_path(self):
        return self.name

    async def delete(self):
        if self.volume:
            await self.volume.delete()
            self.volume = None


def populate_secret_host_path(host_path, secret_data):
    os.makedirs(host_path)
    if secret_data is not None:
        for filename, data in secret_data.items():
            with open(f'{host_path}/{filename}', 'w') as f:
                f.write(base64.b64decode(data).decode())


def copy(files):
    if files is None:
        return 'true'

    authenticate = 'set -ex; gcloud -q auth activate-service-account --key-file=/gsa-key/privateKeyData'

    def copy_command(src, dst):
        if not dst.startswith('gs://'):
            mkdirs = f'mkdir -p {shq(os.path.dirname(dst))};'
        else:
            mkdirs = ""
        return f'{mkdirs} gsutil -m cp -R {shq(src)} {shq(dst)}'

    copies = ' && '.join([copy_command(f['from'], f['to']) for f in files])
    return f'{authenticate} && {copies}'


class BatchPod:
    def __init__(self, worker, parameters, cpu_sem):
        self.worker = worker

        job_spec = parameters['job_spec']

        self.name = parameters['name']
        self.batch_id = parameters['batch_id']
        self.job_id = parameters['job_id']
        self.user = parameters['user']
        self.output_directory = parameters['output_directory']
        self.token = uuid.uuid4().hex
        self.events = []
        self.scratch = f'/batch/pods/{self.name}/{self.token}'

        input_files = job_spec.get('input_files')
        output_files = job_spec.get('output_files')
        pvc_size = job_spec.get('pvc_size')

        # create volumes
        self.volumes = []

        copy_volume_mounts = []
        main_volume_mounts = []

        if job_spec.get('mount_docker_socket'):
            main_volume_mounts.append('/var/run/docker.sock:/var/run/docker.sock')

        if pvc_size or input_files or output_files:
            v = Volume('io')
            self.volumes.append(v)
            volume_mount = 'io:/io'
            main_volume_mounts.append(volume_mount)
            copy_volume_mounts.append(volume_mount)

        secrets = job_spec.get('secrets')
        if secrets:
            for secret in job_spec['secrets']:
                host_path = f'{self.scratch}/{secret["name"]}'
                populate_secret_host_path(host_path, secret['data'])
                volume_mount = f'{host_path}:{secret["mount_path"]}'
                main_volume_mounts.append(volume_mount)
                # this will be the user gsa-key
                if secret.get('mount_in_copy', False):
                    copy_volume_mounts.append(volume_mount)

        # create containers
        def copy_container(files, name):
            sh_expression = copy(files)
            cspec = {
                'image': 'google/cloud-sdk:237.0.0-alpine',
                'name': name,
                'command': ['/bin/sh', '-c', sh_expression],
                'cpu': '500m' if files else '100m',
                'volume_mounts': copy_volume_mounts
            }
            return Container(cspec, self, self.output_directory)

        # main container
        main_cspec = {
            'command': job_spec['command'],
            'image': job_spec['image'],
            'name': 'main',
            # FIXME env
            'cpu': job_spec['resources']['cpu'],
            'volume_mounts': main_volume_mounts
        }

        self.containers = {
            'setup': copy_container(input_files, 'setup'),
            'main': Container(main_cspec, self, self.output_directory),
            'cleanup': copy_container(output_files, 'cleanup')
        }

        self.state = 'Pending'

        self._run_task = asyncio.ensure_future(self.run(cpu_sem))

        self.last_updated = None

    async def _create(self):
        log.info(f'creating pod {self.name}')
        for v in self.volumes:
            await v.create()
        # FIXME: errors not handled properly
        created = await asyncio.gather(*[c.create() for _, c in self.containers.items()])
        return all(created)

    async def _cleanup(self):
        log.info(f'cleaning up pod {self.name}')
        await asyncio.gather(*[c.delete() for _, c in self.containers.items()])
        await asyncio.gather(*[v.delete() for v in self.volumes])
        shutil.rmtree(self.scratch, ignore_errors=True)

    async def _mark_complete(self):
        body = {
            'inst_token': self.worker.token,
            'status': self.to_dict(),
            'events': self.events
        }

        try:
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
                await request_retry_transient_errors(
                    session, 'POST',
                    self.worker.deploy_config.url('batch2-driver', '/api/v1alpha/instances/pod_complete'),
                    json=body)
        except Exception:
            log.exception('failed to mark {self.name} complete')

    async def run(self, semaphore=None):
        start = time.time()
        create_task = None
        try:
            create_task = asyncio.ensure_future(self._create())
            created = await create_task
            if not created:
                log.info(f'unable to create all containers for {self.name}')
                await self._mark_complete()
                return

            self.state = 'Running'

            if not semaphore:
                semaphore = NullWeightedSemaphore()

            last_ec = None
            for _, container in self.containers.items():
                async with semaphore(container.cores_mcpu):
                    log.info(f'running container ({self.name}, {container.name}) with {container.cores_mcpu / 1000} cores')
                    await container.run()
                    last_ec = container.exit_code
                    log.info(f'ran container {container.id} with exit code {container.exit_code} and error {container.error}')
                    if container.error or last_ec != 0:  # Docker sets exit code to 0 by default even if container errors
                        break

            self.state = 'Succeeded' if last_ec == 0 else 'Failed'

            await self._mark_complete()
            log.info(f'took {time.time() - start} seconds to run pod {self.name}')

        except asyncio.CancelledError:
            log.info(f'pod {self.name} was cancelled')
            if create_task is not None:
                await create_task
            raise

    async def delete(self):
        log.info(f'deleting pod {self.name}')
        self._run_task.cancel()
        try:
            await self._run_task
        finally:
            await self._cleanup()

    async def log(self, container_name):
        c = self.containers[container_name]
        return await c.log()

    def container_status(self, container_name):
        c = self.containers[container_name]
        return c.status

    def to_dict(self):
        return {
            'name': self.name,
            'batch_id': self.batch_id,
            'job_id': self.job_id,
            'user': self.user
            'status': {
                'state': self.state
                'container_statuses': [c.to_dict() for _, c in self.containers.items()],
            }
        }


class Worker:
    def __init__(self, image, cores, deploy_config, token, ip_address):
        self.image = image
        self.cores_mcpu = cores * 1000
        self.deploy_config = deploy_config
        self.token = token
        self.free_cores_mcpu = self.cores_mcpu
        self.last_updated = time.time()
        self.pods = {}
        self.cpu_sem = WeightedSemaphore(self.cores_mcpu)
        self.ip_address = ip_address

        pool = concurrent.futures.ThreadPoolExecutor()

        self.gcs_client = GCS(pool)

    async def _create_pod(self, parameters):
        try:
            bp = BatchPod(self, parameters, self.cpu_sem)
            self.pods[bp.name] = bp
        except DockerError as err:
            log.exception(err)
            raise err
            # return web.Response(body=err.message, status=err.status)
        except Exception as err:
            log.exception(err)
            raise err

    async def create_pod(self, request):
        self.last_updated = time.time()
        parameters = await request.json()
        await asyncio.shield(self._create_pod(parameters))
        return web.Response()

    async def get_container_log(self, request):
        pod_name = request.match_info['pod_name']
        container_name = request.match_info['container_name']

        if pod_name not in self.pods:
            raise web.HTTPNotFound(reason='unknown pod name')
        bp = self.pods[pod_name]

        if container_name not in bp.containers:
            raise web.HTTPNotFound(reason='unknown container name')
        result = await bp.log(container_name)

        return web.json_response(result)

    async def get_container_status(self, request):
        pod_name = request.match_info['pod_name']
        container_name = request.match_info['container_name']

        if pod_name not in self.pods:
            raise web.HTTPNotFound(reason='unknown pod name')
        bp = self.pods[pod_name]

        if container_name not in bp.containers:
            raise web.HTTPNotFound(reason='unknown container name')
        result = bp.container_status(container_name)

        return web.json_response(result)

    async def get_pod(self, request):
        pod_name = request.match_info['pod_name']
        if pod_name not in self.pods:
            raise web.HTTPNotFound(reason='unknown pod name')
        bp = self.pods[pod_name]
        return web.json_response(bp.to_dict())

    async def _delete_pod(self, request):
        pod_name = request.match_info['pod_name']

        if pod_name not in self.pods:
            raise web.HTTPNotFound(reason='unknown pod name')
        bp = self.pods[pod_name]
        del self.pods[pod_name]

        asyncio.ensure_future(bp.delete())

    async def delete_pod(self, request):  # pylint: disable=unused-argument
        await asyncio.shield(self._delete_pod(request))
        return web.Response()

    async def list_pods(self, request):  # pylint: disable=unused-argument
        pods = [pod.to_dict() for _, pod in self.pods.items()]
        return web.json_response(pods)

    async def healthcheck(self, request):  # pylint: disable=unused-argument
        return web.Response()

    async def run(self):
        app_runner = None
        site = None
        try:
            app = web.Application()
            app.add_routes([
                web.post('/api/v1alpha/pods/create', self.create_pod),
                web.get('/api/v1alpha/pods/{pod_name}/containers/{container_name}/log', self.get_container_log),
                web.get('/api/v1alpha/pods/{pod_name}/containers/{container_name}/status', self.get_container_status),
                web.get('/api/v1alpha/pods/{pod_name}', self.get_pod),
                web.post('/api/v1alpha/pods/{pod_name}/delete', self.delete_pod),
                web.get('/api/v1alpha/pods', self.list_pods),
                web.get('/healthcheck', self.healthcheck)
            ])

            app_runner = web.AppRunner(app)
            await app_runner.setup()
            site = web.TCPSite(app_runner, '0.0.0.0', 5000)
            await site.start()

            await self.register()

            last_ping = time.time() - self.last_updated
            while (self.pods and last_ping < MAX_IDLE_TIME_WITH_PODS) \
                    or last_ping < MAX_IDLE_TIME_WITHOUT_PODS:
                log.info(f'n_pods {len(self.pods)} free_cores {self.free_cores_mcpu / 1000} age {last_ping}')
                await asyncio.sleep(15)
                last_ping = time.time() - self.last_updated

            if self.pods:
                log.info(f'idle {MAX_IDLE_TIME_WITH_PODS} seconds with pods, exiting')
            else:
                log.info(f'idle {MAX_IDLE_TIME_WITHOUT_PODS} seconds with no pods, exiting')

            body = {'inst_token': self.token}
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
                await request_retry_transient_errors(
                    session, 'POST',
                    self.deploy_config.url('batch2-driver', '/api/v1alpha/instances/deactivate'),
                    json=body)
                log.info('deactivated')
        finally:
            log.info('shutting down')
            if site:
                await site.stop()
                log.info('stopped site')
            if app_runner:
                await app_runner.cleanup()
                log.info('cleaned up app runner')

    async def register(self):
        body = {
            'inst_token': self.token,
            'ip_address': self.ip_address
        }
        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            await request_retry_transient_errors(
                session, 'POST',
                self.deploy_config.url('batch2-driver', '/api/v1alpha/instances/activate'),
                json=body)


cores = int(os.environ['CORES'])
namespace = os.environ['NAMESPACE']
inst_token = os.environ['INST_TOKEN']
ip_address = os.environ['INTERNAL_IP']
batch_worker_image = os.environ['BATCH_WORKER_IMAGE']

log.info(f'BATCH_WORKER_IMAGE={batch_worker_image}')

deploy_config = DeployConfig('gce', namespace, {})
worker = Worker(batch_worker_image, cores, deploy_config, inst_token, ip_address)

loop = asyncio.get_event_loop()
loop.run_until_complete(worker.run())
loop.run_until_complete(docker.close())
loop.run_until_complete(loop.shutdown_asyncgens())
loop.close()
log.info(f'closed')
sys.exit(0)
