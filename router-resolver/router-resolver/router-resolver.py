import os
import uvloop
import asyncio
import aiodns
import aiohttp
from aiohttp import web
from kubernetes_asyncio import client, config

uvloop.install()

app = web.Application()
routes = web.RouteTableDef()

@routes.get('/')
async def auth(request):
    app = request.app
    k8s_client = app['k8s_client']
    host = request.headers['Host']
    if not host.endswith('.internal.hail.is'):
        # would prefer 404, but that's not handled by nginx request_auth
        return web.Response(status=403)
    labels = host.split('.')
    namespace = labels[-4]
    try:
        router = await k8s_client.read_namespaced_service('router', namespace)
    except client.rest.ApiException as err:
        if err.status == 404:
            return web.Response(status=403)
        raise
    return web.Response(status=200, headers={'X-Router-IP': router.spec.cluster_ip})

app.add_routes(routes)

async def on_startup(app):
    if 'BATCH_USE_KUBE_CONFIG' in os.environ:
        await config.load_kube_config()
    else:
        config.load_incluster_config()
        app['k8s_client'] = client.CoreV1Api()

app.on_startup.append(on_startup)

web.run_app(app, host='0.0.0.0', port=5000)
