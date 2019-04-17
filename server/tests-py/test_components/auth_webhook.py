#!/usr/bin/env python 
import base64
import json
import ssl
import http.server
from http import HTTPStatus
import traceback
import sys
import multiprocessing
import threading
import requests
from webserver import RequestHandler, WebServer, MkHandlers, Response

"""
Auth Webhook which tries to imitate auth based on bearer token
    - Bearer token is simply the base64 encoding of headers in json format
    - The validation of token is simulated using header X-Hasura-Auth-Mode
    - Add header 'X-Hasura-Auth-Mode: webhook' to the list of headers for successful auth
    - Omit X-Hasura-Auth-Mode header for an unauthorized connection
"""
class Base64HdrsTokenAuth(RequestHandler):

    def handle_headers(self, headers):
        print(headers)
        if 'Authorization' in headers:
            auth = headers['Authorization']
            h = dict()
            if auth.startswith("Bearer "):
                try:
                    h = json.loads(base64.b64decode(auth[7:]).decode("utf-8"))
                    if h.get('X-Hasura-Auth-Mode') == 'webhook':
                        print (h)
                        return Response(HTTPStatus.OK, h)
                    else:
                        print ('Forbidden: Could not find header X-Hasura-Auth-Mode')
                        self.send_response(401)
                        return Response(HTTPStatus.UNAUTHORIZED, '{}')
                except Exception as e:
                    print ('forbidden')
                    print("type error: " + str(e))
                    print(traceback.format_exc())
                    return Response(HTTPStatus.UNAUTHORIZED)
            else:
                print ('Forbidden: Not a bearer token')
                return Response(HTTPStatus.UNAUTHORIZED)
        else:
            print ('Forbidden: Could not find authorization token')
            return Response(HTTPStatus.UNAUTHORIZED)

    def get(self, request):
        return self.handle_headers(request.headers)

    def post(self, request):
        if 'headers' in request.json:
            return self.handle_headers(request.json['headers'])
        else:
            return self.handle_headers({})

class CookieAuth(RequestHandler):
    def get(self, request):
        headers = {k.lower(): v for k, v in request.headers.items()}
        print(headers)
        if 'cookie' in headers and headers['cookie']:
            res = {'x-hasura-role': 'admin'}
            return Response(HTTPStatus.OK, res)
        return Response(HTTPStatus.UNAUTHORIZED)

    def post(self, request):
        headers = {k.lower(): v for k, v in request.json['headers'].items()}
        print(headers)
        if 'cookie' in headers and headers['cookie']:
            res = {'x-hasura-role': 'admin'}
            return Response(HTTPStatus.OK, res)
        return Response(HTTPStatus.UNAUTHORIZED)


auth_handlers = MkHandlers({
    '/cookie-auth': CookieAuth,
    '/token-as-base64-of-headers' : Base64HdrsTokenAuth
})

def verify_auth_webhook(url):
    if url.endswith('token-as-base64-of-headers'):
        to_enc_hdr = { 'X-Hasura-Auth-Mode' : 'webhook' }
        auth_token = base64.b64encode(json.dumps(to_enc_hdr).encode('utf-8')).decode('utf-8')
        headers = { 'Authorization' : 'Bearer ' + auth_token }
        resp = requests.get(url, headers = headers)
        assert resp.status_code == 200, repr(resp)
    elif url.endswith('cookie-auth'):
        headers =  {'cookie': 'foo=bar'}
        resp = requests.get(url, headers = headers)
        assert resp.status_code == 200, repr(resp)

#class AuthWebhookServer(multiprocessing.Process):
#
#    def __init__(self, keyfile, certfile, stdout_file=None, stderr_file=None, server_address=('127.0.0.1',9090)):
#        super(AuthWebhookServer, self).__init__()
#        self.stdout_file = None
#        if stdout_redirect:
#            print("Auth webhook stdout file: " + stdout_file)
#            self.stdout_file = open(stdout_file, 'w')
#        self.stderr_file = None
#        if stderr_redirect:
#            print("Auth webhook stderr file: " + stderr_file)
#            self.stderr_file = open(stderr_file, 'w')
#        self.events = multiprocessing.Event()
#        self.certfile = certfile
#        self.keyfile = keyfile
#        self.server_address = server_address
#
#    def run(self):
#        if self.stdout_file:
#            sys.stdout = self.stdout_file
#        if self.stderr_file
#            sys.stderr = self.stderr_file
#        self.auth_webhook_httpd = http.server.HTTPServer(self.server_address, AuthWebhook)
#        self.auth_webhook_httpd.socket = ssl.wrap_socket (
#            self.auth_webhook_httpd.socket,
#            certfile=self.certfile,
#            keyfile=self.keyfile,
#            server_side=True,
#            ssl_version=ssl.PROTOCOL_SSLv23
#        )
#        self.auth_webhook_server = threading.Thread(target=self.auth_webhook_httpd.serve_forever)
#        self.auth_webhook_server.start()
#        self.wait_for_terminate()
#
#    def wait_for_terminate(self):
#        self.events.wait()
#        print  ('AuthWebhook: Recieved terminate event')
#        self.teardown()
#
#    def stop(self):
#        self.events.set()
#
#    def teardown(self):
#        self.auth_webhook_httpd.shutdown()
#        self.auth_webhook_httpd.server_close()
#        self.auth_webhook_httpd.socket.close()
#        self.auth_webhook_server.join()
#        if self.log_file:
#            self.log_file.close()
#
#def run(keyfile, certfile, server_class=http.server.HTTPServer, handler_class=AuthWebhook, port=9090):
#    server_address = ('127.0.0.1', port)
#    httpd = server_class(server_address, handler_class)
#    httpd.socket = ssl.wrap_socket (
#        httpd.socket,
#        certfile=certfile,
#        keyfile=keyfile,
#        server_side=True,
#        ssl_version=ssl.PROTOCOL_SSLv23)
#    print('Starting httpd...')
#    httpd.serve_forever()
#
#if __name__ == "__main__":
#
#    if len(sys.argv) != 4:
#        print("Usage: python webhook.py port keyfile certfile")
#        sys.exit(1)
#    run(keyfile=sys.argv[2], certfile=sys.argv[3], port=int(sys.argv[1]))
