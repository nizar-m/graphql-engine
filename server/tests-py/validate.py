#!/usr/bin/env python3

import yaml
import json
import os
import base64
import jsondiff
import jwt
import random
import time
from fixture_modules.hge_websocket_client import hge_ws_client

def check_keys(keys, obj):
    for k in keys:
        assert k in obj, obj


def check_ev_payload_shape(ev_payload):
    top_level_keys = ["created_at", "event", "id", "table", "trigger"]
    check_keys(top_level_keys, ev_payload)

    event_keys = ["data", "op"]
    check_keys(event_keys, ev_payload['event'])

    trigger_keys = ["name"]
    check_keys(trigger_keys, ev_payload['trigger'])


def validate_event_payload(ev_payload, trig_name, table):
    check_ev_payload_shape(ev_payload)
    assert ev_payload['table'] == table, ev_payload
    assert ev_payload['trigger']['name'] == trig_name, ev_payload


def validate_event_headers(ev_headers, headers):
    for key, value in headers.items():
        v = ev_headers.get(key)
        assert v == value, (key, v)


def validate_event_webhook(ev_webhook_path, webhook_path):
    assert ev_webhook_path == webhook_path


def check_event(hge_ctx, evts_webhook, trig_name, table, operation, exp_ev_data,
                headers = {},
                webhook_path = '/',
                session_variables = {'x-hasura-role': 'admin'}
):
    ev_full = evts_webhook.get_event(3)
    validate_event_webhook(ev_full['path'], webhook_path)
    validate_event_headers(ev_full['headers'], headers)
    validate_event_payload(ev_full['body'], trig_name, table)
    ev = ev_full['body']['event']
    assert ev['op'] == operation, ev
    assert ev['session_variables'] == session_variables, ev
    assert ev['data'] == exp_ev_data, ev


def assert_not_found(code, resp):
    errMsg = dump_explicit_yaml({
        "expected": "404 - Not found",
        "actual": {
            "code": code,
            "response": resp
        }
    })
    assert code == 404, errMsg


def assert_access_denied(code, resp, exp_code):
    deniedMsg = dump_explicit_yaml({
        "expected": "Should be access denied as admin secret is not provided",
        "actual": {
            "code": code,
            "response": resp
        },
        "exp_code": exp_code
    })
    assert code == exp_code, deniedMsg
    if exp_code == 200:
        errors = resp.get('errors',[])
        assert isinstance(errors, list) and len(errors) > 0, deniedMsg
        assert errors[0].get('extensions',{}).get('code') == 'access-denied', deniedMsg


def assert_access_denied_or_not_found(code, resp, exp_code):
    if exp_code == 404:
        assert_not_found(code, resp)
    else:
        assert_access_denied(code, resp, exp_code)

def get_exp_perm_denied_status(conf):
    if conf['status'] == 404:
        return 404
    elif conf['url'].endswith('graphql'):
        if 'v1alpha1' in conf['url']:
            return 401
        else:
            return 200
    else:
        return 401

def test_forbidden_when_admin_secret_reqd(hge_ctx, conf):
    exp_code = get_exp_perm_denied_status(conf)

    headers = {}
    if 'headers' in conf:
        headers = conf['headers']

    # Test without admin secret
    code, resp = hge_ctx.anyq(conf['url'], conf['query'], headers)
    assert_access_denied_or_not_found(code, resp, exp_code)

    # Test with random admin secret
    headers['X-Hasura-Admin-Secret'] = base64.b64encode(os.urandom(30))
    code, resp = hge_ctx.anyq(conf['url'], conf['query'], headers)
    assert_access_denied_or_not_found(code, resp, exp_code)


def test_forbidden_webhook(hge_ctx, conf):
    exp_code = get_exp_perm_denied_status(conf)

    h = {'Authorization': 'Bearer ' + base64.b64encode(base64.b64encode(os.urandom(30))).decode('utf-8')}
    code, resp = hge_ctx.anyq(conf['url'], conf['query'], h)
    assert_access_denied_or_not_found(code, resp, exp_code)


def get_claims_fmt(hge_ctx):
    conf = json.loads(hge_ctx.hge_jwt_conf)
    return conf.get('claims_format', 'json')


def mk_claims(hge_ctx, claims):
    claims_fmt = get_claims_fmt(hge_ctx)
    if claims_fmt == 'stringified_json':
        return json.dumps(claims)
    else:
        return claims


def generate_jwt_token(hge_ctx, headers):
    hClaims = dict()
    hClaims['X-Hasura-Allowed-Roles'] = [headers['X-Hasura-Role']]
    hClaims['X-Hasura-Default-Role'] = headers['X-Hasura-Role']
    for key in headers:
        if key != 'X-Hasura-Role':
            hClaims[key] = headers[key]
    claim = {
        "sub": "foo",
        "name": "bar",
        "https://hasura.io/jwt/claims": mk_claims(hge_ctx, hClaims)
    }
    return jwt.encode(claim, hge_ctx.hge_jwt_key, algorithm='RS512').decode('UTF-8')


def do_invalid_http_auth_tests(hge_ctx, conf):
    headers = conf.get('headers',{})
    secure_webhook_auth = hge_ctx.hge_webhook and len(headers) > 0 and not hge_ctx.webhook_insecure
    admin_secret_reqd = hge_ctx.hge_key and not hge_ctx.hge_webhook and not hge_ctx.hge_jwt_key
    if secure_webhook_auth:
        test_forbidden_webhook(hge_ctx, conf)
    elif admin_secret_reqd:
        test_forbidden_when_admin_secret_reqd(hge_ctx, conf)


def add_auth_hdrs(hge_ctx, headers):
    new_hdrs = {}
    set_jwt_auth = hge_ctx.hge_jwt_key and len(headers) > 0 and 'X-Hasura-Role' in headers
    set_webhook_auth = hge_ctx.hge_webhook and len(headers) > 0
    set_admin_secret = hge_ctx.hge_key and not (set_jwt_auth or set_webhook_auth)
    if set_jwt_auth:
        new_hdrs['Authorization'] = 'Bearer ' +  generate_jwt_token(hge_ctx, headers)
    elif set_webhook_auth:
        headers['X-Hasura-Auth-Mode'] = 'webhook'
        token = base64.b64encode(json.dumps(headers).encode('utf-8')).decode('utf-8')
        new_hdrs['Authorization'] = 'Bearer ' + token
    else:
        new_hdrs = headers
        if set_admin_secret:
            new_hdrs['X-Hasura-Admin-Secret'] = hge_ctx.hge_key
    return new_hdrs

def check_query(hge_ctx, conf, transport='http', add_auth=True):
    headers = conf.get('headers',{})

    # No headers defined in test configuration implies X-Hasura-Role = admin
    # Set `X-Hasura-Role: admin` header randomly
    if len(headers) == 0 and random.choice([True, False]):
        headers['X-Hasura-Role'] = 'admin'

    if add_auth:
        if transport == 'http':
            do_invalid_http_auth_tests(hge_ctx, conf)
        headers = add_auth_hdrs(hge_ctx, headers)

    assert transport in ['websocket', 'http'], "Unknown transport type " + transport
    if transport == 'websocket':
        assert 'response' in conf, conf
        endpoint = conf['url']
        assert endpoint.endswith('/graphql')
        print('running on websocket')
        with hge_ws_client(hge_ctx, endpoint) as ws_client:
            return validate_gql_ws_q(hge_ctx, ws_client, conf['query'], headers, conf['response'], True)
    elif transport == 'http':
        print('running on http')
        return validate_http_anyq(hge_ctx, conf['url'], conf['query'], headers,
                                  conf['status'], conf.get('response'))


def validate_gql_ws_q(hge_ctx, ws_client, query, headers, exp_http_response, retry=False):
    print(ws_client.ws_url)
    ws_client.init(headers)
    query_resp = ws_client.send_query(query, timeout=15)
    resp = next(query_resp)
    print('websocket resp: ', resp)

    if resp.get('type') == 'complete':
        if not retry:
            assert resp['type'] in ['data', 'error'], resp
        else:
            #Got query complete before payload. Retry once more
            print("Got query complete before getting query response payload. Retrying")
            ws_client.recreate_conn()
            time.sleep(3)
            return validate_gql_ws_q(hge_ctx, ws_client, query, headers, exp_http_response, False)

    if 'errors' in exp_http_response or 'error' in exp_http_response:
        assert resp['type'] in ['data', 'error'], resp
    else:
        assert resp['type'] == 'data', resp

    exp_ws_payload = exp_http_response

    assert 'payload' in resp, resp
    validate_json(resp['payload'], exp_ws_payload) 
    resp_done = next(query_resp)
    assert resp_done['type'] == 'complete'
    return resp['payload']


def validate_http_anyq(hge_ctx, url, query, headers, exp_code, exp_response):
    code, resp = hge_ctx.anyq(url, query, headers)
    print(headers)
    assert code == exp_code, dump_explicit_yaml ({
        'query': query,
        'response' : resp
    })
    if exp_response:
        validate_json(resp, exp_response)
    return resp


def check_query_yaml_conf(hge_ctx, conf, transport, add_auth):
    print ("transport="+transport)
    hge_ctx.may_skip_test_teardown = False
    if isinstance(conf, list):
        for sconf in conf:
            check_query(hge_ctx, sconf, transport, add_auth)
    else:
        hge_ctx.may_skip_test_teardown = conf['status'] != 200
        check_query(hge_ctx, conf, transport, add_auth)


def json_equals(resp, exp):
    return jsondiff.diff(resp, exp) == {}


def validate_json(resp, exp):
    diff = jsondiff.diff(resp, exp)
    assert diff == {}, '\n' + dump_explicit_yaml({
      'response' : resp,
      'expected' : exp,
      'diff' : diff
    })


class ExplicitYamlDumper(yaml.Dumper):
    """
    A dumper that will never emit aliases.
    """
    def ignore_aliases(self, data):
        return True


def dump_explicit_yaml(obj):
    return yaml.dump(obj, Dumper=ExplicitYamlDumper)
