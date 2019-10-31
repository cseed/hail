import asyncio
import sortedcontainers
import logging
import googleapiclient.errors

from ..utils import new_token
from ..batch_configuration import BATCH_NAMESPACE, BATCH_WORKER_IMAGE, INSTANCE_ID, \
    PROJECT, ZONE, WORKER_TYPE, WORKER_CORES, WORKER_DISK_SIZE_GB, \
    POOL_SIZE, MAX_INSTANCES

from .instance import Instance

log = logging.getLogger('instance_pool')

WORKER_CORES_MCPU = WORKER_CORES * 1000

log.info(f'WORKER_CORES {WORKER_CORES}')
log.info(f'WORKER_TYPE {WORKER_TYPE}')
log.info(f'WORKER_DISK_SIZE_GB {WORKER_DISK_SIZE_GB}')
log.info(f'POOL_SIZE {POOL_SIZE}')
log.info(f'MAX_INSTANCES {MAX_INSTANCES}')


class InstancePool:
    def __init__(self, app, machine_name_prefix):
        self.app = app
        self.log_root = app['log_root']
        self.scheduler_state_changed = app['scheduler_state_changed']
        self.db = app['db']
        self.gservices = app['gservices']
        self.k8s = app['k8s_client']
        self.machine_name_prefix = machine_name_prefix

        if WORKER_TYPE == 'standard':
            m = 3.75
        elif WORKER_TYPE == 'highmem':
            m = 6.5
        else:
            assert WORKER_TYPE == 'highcpu', WORKER_TYPE
            m = 0.9
        self.worker_memory = 0.9 * m

        # active instances only
        self.active_instances_by_free_cores = sortedcontainers.SortedSet(key=lambda inst: inst.free_cores_mcpu)

        self.n_instances_by_state = {
            'pending': 0,
            'active': 0,
            'inactive': 0,
            'deleted': 0
        }

        # pending and active
        self.live_free_cores_mcpu = 0

        self.id_instance = {}

        self.token_inst = {}

    @staticmethod
    def worker_log_path(log_root, machine_name):
        return f'{log_root}/worker/{machine_name}/worker.log'

    async def async_init(self):
        log.info('initializing instance pool')

        async for record in self.db.execute_and_fetchall(
                'SELECT * FROM instances;'):
            instance = Instance.from_record(self.app, record)
            self.add_instance(instance)

        asyncio.ensure_future(self.event_loop())
        asyncio.ensure_future(self.control_loop())

    @property
    def n_instances(self):
        return len(self.token_inst)

    def adjust_for_remove_instance(self, instance):
        self.n_instances_by_state[instance.state] -= 1

        if instance.state in ('pending', 'active'):
            self.live_free_cores_mcpu -= instance.free_cores_mcpu
        if instance.state == 'active':
            self.active_instances_by_free_cores.remove(instance)

    async def remove_instance(self, instance):
        await self.db.just_execute(
            'DELETE FROM instances WHERE id = %s;', (instance.id,))

        self.adjust_for_remove_instance(instance)

        del self.token_inst[instance.token]
        del self.id_instance[instance.id]

    def adjust_for_add_instance(self, instance):
        self.n_instances_by_state[instance.state] += 1

        if instance.state in ('pending', 'active'):
            self.live_free_cores_mcpu += instance.free_cores_mcpu
        if instance.state == 'active':
            self.active_instances_by_free_cores.add(instance)

    def add_instance(self, instance):
        assert instance.token not in self.token_inst
        self.token_inst[instance.token] = instance
        self.id_instance[instance.id] = instance

        self.adjust_for_add_instance(instance)

    async def create_instance(self):
        while True:
            inst_token = new_token()
            if inst_token not in self.token_inst:
                break
        machine_name = f'{self.machine_name_prefix}{inst_token}'

        instance = await Instance.create(self.app, machine_name, inst_token, WORKER_CORES_MCPU)
        self.add_instance(instance)

        log.info(f'created {instance}')

        config = {
            'name': machine_name,
            'machineType': f'projects/{PROJECT}/zones/{ZONE}/machineTypes/n1-{WORKER_TYPE}-{WORKER_CORES}',
            'labels': {
                'role': 'batch2-agent',
                'inst_token': inst_token,
                'batch_instance': INSTANCE_ID,
                'namespace': BATCH_NAMESPACE
            },

            'disks': [{
                'boot': True,
                'autoDelete': True,
                'diskSizeGb': WORKER_DISK_SIZE_GB,
                'initializeParams': {
                    'sourceImage': f'projects/{PROJECT}/global/images/batch2-worker-5',
                }
            }],

            'networkInterfaces': [{
                'network': 'global/networks/default',
                'networkTier': 'PREMIUM',
                'accessConfigs': [{
                    'type': 'ONE_TO_ONE_NAT',
                    'name': 'external-nat'
                }]
            }],

            'scheduling': {
                'automaticRestart': False,
                'onHostMaintenance': "TERMINATE",
                'preemptible': True
            },

            'serviceAccounts': [{
                'email': 'batch2-agent@hail-vdc.iam.gserviceaccount.com',
                'scopes': [
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            }],

            'metadata': {
                'items': [{
                    'key': 'startup-script',
                    'value': f'''
#!/bin/bash
set -ex

export BATCH_WORKER_IMAGE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_worker_image")
export HOME=/root

function retry {{
    local n=1
    local max=5
    local delay=15
    while true; do
        "$@" > worker.log 2>&1 && break || {{
            if [[ $n -lt $max ]]; then
                ((n++))
                echo "Command failed. Attempt $n/$max:"
                sleep $delay;
            else
                export INST_TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/inst_token")
                export LOG_ROOT=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/log_root")
                export NAME=$(curl http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
                export ZONE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')

                echo "startup of batch worker failed after $n attempts;" >> worker.log
                gsutil -m cp worker.log {self.worker_log_path("$LOG_ROOT", "$NAME")}

                gcloud -q compute instances delete $NAME --zone=$ZONE
             fi
        }}
    done
}}

retry docker run \
           -v /var/run/docker.sock:/var/run/docker.sock \
           -v /usr/bin/docker:/usr/bin/docker \
           -v /batch:/batch \
           -p 5000:5000 \
           -d --entrypoint "/bin/bash" \
           $BATCH_WORKER_IMAGE \
           -c "sh /run-worker.sh"
'''
                }, {
                    'key': 'inst_token',
                    'value': inst_token
                }, {
                    'key': 'batch_worker_image',
                    'value': BATCH_WORKER_IMAGE
                }, {
                    'key': 'batch_instance',
                    'value': INSTANCE_ID
                }, {
                    'key': 'namespace',
                    'value': BATCH_NAMESPACE
                }, {
                    'key': 'log_root',
                    'value': self.log_root
                }]
            },
            'tags': {
                'items': [
                    "batch2-agent"
                ]
            },
        }

        await self.gservices.create_instance(config)
        log.info(f'created machine {machine_name} with logs at {self.worker_log_path(self.log_root, machine_name)}')

    async def call_delete_instance(self, instance, force=False):
        if instance.state == 'deleted' and not force:
            return
        if instance.state not in ('inactive', 'deleted'):
            await instance.deactivate()

        try:
            await self.gservices.delete_instance(instance.name)
        except googleapiclient.errors.HttpError as e:
            if e.resp['status'] == '404':
                log.info(f'{instance} already delete done')
                await self.remove_instance(instance)
                return
            raise

    async def handle_preempt_event(self, instance):
        await self.call_delete_instance(instance)

    async def handle_delete_done_event(self, instance):
        await self.remove_instance(instance)

    async def handle_call_delete_event(self, instance):
        await instance.mark_deleted()

    async def handle_event(self, event):
        if not event.payload:
            log.warning(f'event has no payload')
            return

        payload = event.payload
        version = payload['version']
        if version != '1.2':
            log.warning('unknown event verison {version}')
            return

        resource_type = event.resource.type
        if resource_type != 'gce_instance':
            log.warning(f'unknown event resource type {resource_type}')
            return

        event_type = payload['event_type']
        event_subtype = payload['event_subtype']
        resource = payload['resource']
        name = resource['name']

        log.info(f'event {version} {resource_type} {event_type} {event_subtype} {name}')

        if not name.startswith(self.machine_name_prefix):
            log.warning(f'event for unknown machine {name}')
            return

        inst_token = name[len(self.machine_name_prefix):]
        instance = self.token_inst.get(inst_token)
        if not instance:
            log.warning(f'event for unknown instance {inst_token}')
            return

        if event_subtype == 'compute.instances.preempted':
            log.info(f'event handler: handle preempt {instance}')
            await self.handle_preempt_event(instance)
        elif event_subtype == 'compute.instances.delete':
            if event_type == 'GCE_OPERATION_DONE':
                log.info(f'event handler: delete {instance} done')
                await self.handle_delete_done_event(instance)
            elif event_type == 'GCE_API_CALL':
                log.info(f'event handler: handle call delete {instance}')
                await self.handle_call_delete_event(instance)
            else:
                log.warning(f'unknown event type {event_type}')
        else:
            log.warning(f'unknown event subtype {event_subtype}')

    async def event_loop(self):
        log.info(f'starting event loop')
        while True:
            try:
                async for event in await self.gservices.stream_entries():
                    await self.handle_event(event)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:
                log.exception('event loop failed due to exception')
            await asyncio.sleep(15)

    async def control_loop(self):
        log.info(f'starting control loop')
        while True:
            try:
                ready_cores = await self.db.execute_and_fetchone(
                    'SELECT * FROM ready_cores;')
                ready_cores_mcpu = ready_cores['ready_cores_mcpu']

                log.info(f'n_instances {self.n_instances} {self.n_instances_by_state}'
                         f' live_free_cores {self.live_free_cores_mcpu / 1000}'
                         f' ready_cores {ready_cores_mcpu / 1000}')

                if ready_cores_mcpu > 0:
                    n_live_instances = self.n_instances_by_state['pending'] + self.n_instances_by_state['active']
                    instances_needed = (ready_cores_mcpu - self.live_free_cores_mcpu + WORKER_CORES_MCPU - 1) // WORKER_CORES_MCPU
                    instances_needed = min(instances_needed,
                                           POOL_SIZE - n_live_instances,
                                           MAX_INSTANCES - self.n_instances,
                                           # 20 queries/s; our GCE long-run quota
                                           300)
                    if instances_needed > 0:
                        log.info(f'creating {instances_needed} new instances')
                        # parallelism will be bounded by thread pool
                        await asyncio.gather(*[self.create_instance() for _ in range(instances_needed)])
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:
                log.exception('instance pool control loop: caught exception')

            await asyncio.sleep(15)
