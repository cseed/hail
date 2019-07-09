import json
import logging
import base64
import secrets
import aiohttp
from aiohttp import web
import aiohttp_session
import aiohttp_session.cookie_storage
import aiomysql
import uvloop

import google.auth.transport.requests
import google.oauth2.id_token
import google_auth_oauthlib.flow

from hailtop.gear import get_deploy_config
from hailtop.gear.auth import get_jwtclient, authenticated_users_only, create_session_token

log = logging.getLogger('auth')

uvloop.install()

deploy_config = get_deploy_config()

BASE_PATH = deploy_config.base_path('auth')
log.info(f'BASE_PATH {BASE_PATH}')

routes = web.RouteTableDef()


def get_flow(redirect_uri, state=None):
    scopes = [
        'https://www.googleapis.com/auth/userinfo.profile',
        'https://www.googleapis.com/auth/userinfo.email',
        'openid'
    ]
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        '/auth-oauth2-client-secret/client_secret.json', scopes=scopes, state=state)
    flow.redirect_uri = redirect_uri
    return flow


@routes.get(f'/healthcheck')
async def get_unprefixed_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


@routes.get(f'{BASE_PATH}/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


@routes.get(f'{BASE_PATH}/')
async def get_index(request):  # pylint: disable=unused-argument
    return aiohttp.web.HTTPFound('/login')


@routes.get(f'{BASE_PATH}/login')
async def login(request):
    flow = get_flow(deploy_config.external_url('auth', '/oauth2callback'))

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true')

    session = await aiohttp_session.get_session(request)
    session['state'] = state
    next = request.query.get('next')
    if next:
        session['next'] = next
    elif 'next' in session:
        del session['next']

    return aiohttp.web.HTTPFound(authorization_url)


@routes.get(f'{BASE_PATH}/oauth2callback')
async def callback(request):
    unauth = web.HTTPUnauthorized(headers={'WWW-Authenticate': 'Bearer'})

    session = await aiohttp_session.get_session(request)
    if 'state' not in session:
        raise unauth
    state = session['state']

    flow = get_flow(deploy_config.external_url('auth', '/oauth2callback'), state=state)

    try:
        flow.fetch_token(code=request.query['code'])
        token = google.oauth2.id_token.verify_oauth2_token(
            flow.credentials.id_token, google.auth.transport.requests.Request())
        id = token['sub']
    except Exception:
        log.exception('oauth2 callback: could not fetch and verify token')
        raise unauth

    dbpool = request.app['dbpool']
    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT * from users.user_data where user_id = %s;', f'google-oauth2|{id}')
            users = await cursor.fetchall()

    if len(users) != 1:
        raise unauth
    user = users[0]

    session_id = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('ascii')

    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('INSERT INTO users.sessions (session_id, kind, user_id, max_age_secs) VALUES (%s, %s, %s, %s);',
                                 # 2592000s = 30d
                                 (session_id, 'web', user['id'], 2592000))

    del session['state']
    next = session.pop('next', deploy_config.external_url('notebook2', ''))
    session['session_id'] = session_id
    return aiohttp.web.HTTPFound(next)


@routes.post(f'{BASE_PATH}/logout')
@authenticated_users_only
async def logout(request, userdata):
    dbpool = request.app['dbpool']
    session_id = userdata['session_id']
    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('DELETE FROM user.sessions WHERE session_id = %s;', session_id)

    session = await aiohttp_session.get_session(request)
    if session:
        session.invalidate()

    # FIXME redirect to a nice place
    return web.Response(status=200)


@routes.get(f'{BASE_PATH}/api/v1alpha/login')
async def rest_login(request):
    callback_port = request.query['callback_port']

    flow = get_flow(f'http://127.0.0.1:{callback_port}/oauth2callback')
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true')

    return web.json_response({
        'authorization_url': authorization_url,
        'state': state
    })


@routes.get(f'{BASE_PATH}/api/v1alpha/oauth2callback')
async def rest_callback(request):
    unauth = web.HTTPUnauthorized(headers={'WWW-Authenticate': 'Bearer'})

    state = request.query['state']
    code = request.query['code']
    callback_port = request.query['callback_port']

    try:
        flow = get_flow(f'http://127.0.0.1:{callback_port}/oauth2callback', state=state)
        flow.fetch_token(code=code)
        token = google.oauth2.id_token.verify_oauth2_token(
            flow.credentials.id_token, google.auth.transport.requests.Request())
        id = token['sub']
    except Exception:
        log.exception('fetching and decoding token')
        raise unauth

    dbpool = request.app['dbpool']
    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT * from users.user_data where user_id = %s;', f'google-oauth2|{id}')
            users = await cursor.fetchall()

    if len(users) != 1:
        raise unauth
    user = users[0]

    session_id = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('ascii')

    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('INSERT INTO users.sessions (session_id, kind, user_id, max_age_secs) VALUES (%s, %s, %s, %s);',
                                 (session_id, 'rest', user['id'], 30 * 86400))

    return web.json_response({
        'token': create_session_token(session_id),
        'username': user['username']
    })


@routes.post(f'{BASE_PATH}/api/v1alpha/logout')
@authenticated_users_only
async def rest_logout(request, userdata):
    session_id = userdata['session_id']
    dbpool = request.app['dbpool']
    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('DELETE FROM user.sessions WHERE session_id = %s;', session_id)

    return web.Response(status=200)


@routes.get(f'{BASE_PATH}/api/v1alpha/userinfo')
async def userinfo(request):
    auth_header = request.headers.get('Authorization')
    if auth_header:
        if not auth_header.startswith('Bearer '):
            raise web.HTTPUnauthorized()
        jwt = auth_header[6:]
        try:
            body = get_jwtclient().decode(jwt)
        except jwt.InvalidTokenError:
            raise web.HTTPUnauthorized()

        session_id = body['session_id']
    else:
        session = await aiohttp_session.get_session(request)
        session_id = session.get('session_id')
        if not session_id:
            raise web.HTTPUnauthorized()

    # b64 encoding of 32-byte session ID is 44 bytes
    if len(session_id) != 44:
        raise web.HTTPUnauthorized()

    dbpool = request.app['dbpool']
    async with dbpool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('''
SELECT user_data.* FROM user_data
INNER JOIN sessions ON user_data.id = sessions.user_id
WHERE (sessions.session_id = %s) AND (ISNULL(sessions.max_age_secs) OR (TIMESTAMPADD(SECOND, sessions.max_age_secs, sessions.created) < NOW()));
''', session_id)
            users = await cursor.fetchall()

    if len(users) != 1:
        raise web.HTTPUnauthorized()
    user = users[0]

    return web.json_response(user)


async def on_startup(app):
    with open('/sql-users-users-user-config/sql-config.json', 'r') as f:
        config = json.loads(f.read().strip())
        app['dbpool'] = await aiomysql.create_pool(host=config['host'],
                                                   port=config['port'],
                                                   db=config['db'],
                                                   user=config['user'],
                                                   password=config['password'],
                                                   charset='utf8',
                                                   cursorclass=aiomysql.cursors.DictCursor,
                                                   autocommit=True)


async def on_cleanup(app):
    dbpool = app['dbpool']
    dbpool.close()
    await dbpool.wait_closed()


def run():
    app = web.Application()

    with open('/session-secret-keys/aiohttp-session-secret-key', 'rb') as f:
        aiohttp_session.setup(app, aiohttp_session.cookie_storage.EncryptedCookieStorage(
            f.read(),
            cookie_name=deploy_config.auth_session_cookie_name(),
            # 2592000s = 30d
            max_age=2592000))

    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, host='0.0.0.0', port=5000)
