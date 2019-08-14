import json
import threading
from urllib.parse import urlparse

import websocket
import pytest
from validate import check_query
from skip_test_modules import skip_module
from conftest import per_class_db_context

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)

def url(hge_ctx):
    ws_url = urlparse(hge_ctx.hge_url)._replace(scheme='ws', path='/v1alpha1/graphql')
    return ws_url.geturl()

@per_class_db_context
class TestWebsocketInitCookie():
    """
    test if cookie is sent when initing the websocket connection, is our auth
    webhook receiving the cookie
    """
    dir = 'queries/remote_schemas'

    setup_files = dir + '/person_table.yaml'

    teardown_files = dir + '/drop_person_table.yaml'

    def _send_query(self, hge_ctx):
        ws_url = url(hge_ctx)
        headers = {'Cookie': 'foo=bar;'}
        ws = websocket.create_connection(ws_url, header=headers)
        init_payload = {
            'type': 'connection_init',
            'payload': {'headers': {}}
        }
        ws.send(json.dumps(init_payload))
        payload = {
            'type': 'start',
            'id': '1',
            'payload': {'query': 'query { person {name}}'}
        }
        ws.send(json.dumps(payload))
        return ws

    def test_websocket_init_cookie_used(self, hge_ctx):
        if hge_ctx.ws_read_cookie == 'noread':
            pytest.skip('cookie is not to be read')
        ws = self._send_query(hge_ctx)
        it = 0
        while True:
            raw = ws.recv()
            frame = json.loads(raw)
            if frame['type'] == 'data':
                assert 'person' in frame['payload']['data']
                break
            elif it == 10:
                print('max try over')
                assert False
                break
            elif frame['type'] == 'connection_error' or frame['type'] == 'error':
                print(frame)
                assert False
                break
            it = it + 1

    def test_websocket_init_cookie_not_used(self, hge_ctx):
        if hge_ctx.ws_read_cookie == 'read':
            pytest.skip('cookie is read')

        ws = self._send_query(hge_ctx)
        it = 0
        while True:
            raw = ws.recv()
            frame = json.loads(raw)
            if frame['type'] ==  'data':
                print('got data')
                assert False
                break
            elif it == 10:
                print('max try over')
                assert False
                break
            elif frame['type'] == 'connection_error':
                print(frame)
                assert frame['payload'] == 'Authentication hook unauthorized this request'
                break
            elif frame['type'] == 'error':
                print(frame)
                assert False
                break
            it = it + 1
