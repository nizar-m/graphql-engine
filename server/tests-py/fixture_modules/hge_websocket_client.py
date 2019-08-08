import threading
import time
import queue
import json
import string
import random
from urllib.parse import urlparse
import graphql
import websocket
from contextlib import contextmanager

@contextmanager
def hge_ws_client(hge_ctx, endpoint):
    client = HgeWsClient(hge_ctx, endpoint)
    try:
        yield client
    finally:
        threading.Thread(target = client.close).start()



class HgeWsClient:

    def __init__(self, hge_ctx, endpoint):
        self.hge_ctx = hge_ctx
        self.ws_queue = queue.Queue(maxsize=-1)
        self.ws_url = urlparse(hge_ctx.hge_url)
        self.ws_url = self.ws_url._replace(scheme='ws', path=endpoint)
        self.create_conn()

    def create_conn(self):
        self.connected_evt = threading.Event()
        self.ws_queue.queue.clear()
        self.ws_id_query_queues = dict()
        self.ws_active_query_ids = set()
        self._ws = websocket.WebSocketApp(self.ws_url.geturl(), on_message=self._on_message, on_close=self._on_close)
        self.wst = threading.Thread(target=self._ws.run_forever)
        self.wst.daemon = True
        self.remote_closed = False
        self.init_done = False
        self.wst.start()
        print("Creating websocket connection. url:", self.ws_url.geturl())
        self.connected_evt.wait(5)

    def recreate_conn(self):
        self.close()
        self.create_conn()

    def get_ws_event(self, timeout):
        return self.ws_queue.get(timeout=timeout)

    def has_ws_query_events(self, query_id):
        return not self.ws_id_query_queues[query_id].empty()

    def get_ws_query_event(self, query_id, timeout):
        return self.ws_id_query_queues[query_id].get(timeout=timeout)

    def connected(self):
        return self.connected_evt.isSet()

    def send(self, frame):
        if not self.connected():
            print ("Recreating connection")
            self.recreate_conn()
            time.sleep(1)
        if frame.get('type') == 'stop':
            self.ws_active_query_ids.discard( frame.get('id') )
        elif frame.get('type') == 'start' and 'id' in frame:
            self.ws_id_query_queues[frame['id']] = queue.Queue(maxsize=-1)
        self._ws.send(json.dumps(frame))

    def init_as_admin(self):
        headers={}
        if self.hge_ctx.hge_key:
            headers = {'x-hasura-admin-secret': self.hge_ctx.hge_key}
        self.init(headers)

    def init(self, headers={}):
        payload = {'type': 'connection_init', 'payload': {}}

        if headers and len(headers) > 0:
            payload['payload']['headers'] = headers

        self.send(payload)
        ev = self.get_ws_event(3)
        assert ev['type'] == 'connection_ack', ev
        self.init_done = True

    def stop(self, query_id):
        data = {'id': query_id, 'type': 'stop'}
        self.send(data)
        self.ws_active_query_ids.discard(query_id)

    def gen_id(self, size=6, chars=string.ascii_letters + string.digits):
        newId = ''.join(random.choice(chars) for _ in range(size))
        if newId in self.ws_active_query_ids:
            return self.gen_id(size,chars)
        else:
            return newId

    def send_query(self, query, query_id=None, headers={}, timeout=60):
        graphql.parse(query['query'])
        if headers and len(headers) > 0:
            #Do init If headers are provided
            self.init(headers)
        elif not self.init_done:
            self.init()
        if query_id == None:
            query_id = self.gen_id()
        frame = {
            'id': query_id,
            'type': 'start',
            'payload': query,
        }
        self.ws_active_query_ids.add(query_id)
        self.send(frame)
        while True:
            yield self.get_ws_query_event(query_id, timeout)

    def _on_open(self):
        self.connected_evt.set()

    def _on_message(self, message):
        self.connected_evt.set()
        json_msg = json.loads(message)
        if 'id' in json_msg:
            query_id = json_msg['id']
            if json_msg.get('type') == 'stop':
                #Remove from active queries list
                self.ws_active_query_ids.discard( query_id )
            if not query_id in self.ws_id_query_queues:
                self.ws_id_query_queues[json_msg['id']] = queue.Queue(maxsize=-1)
            #Put event in the correponding query_queue
            self.ws_id_query_queues[query_id].put(json_msg)
        elif json_msg['type'] == 'ka':
            pass
        else:
            #Put event in the main queue
            self.ws_queue.put(json_msg)

    def _on_close(self):
        print("Received remote close message")
        self.remote_closed = True
        self.connected_evt.clear()
        self.init_done = False

    def close(self):
        print("Closing websocket")
        if not self.remote_closed:
            self._ws.close()
        self.wst.join()
