import pytest
import os

_to_skip_test_modules = dict()

def skip_module(filename):
    return _to_skip_test_modules.get(os.path.basename(filename))

def set_skip_test_module_rules(config):
    def skip_reason(s):
        return s + ". Skipping tests in this module"
    def set_skip_reason(reason, modules):
        if isinstance(modules, str):
            return set_skip_reason(reason, [modules])
        for m in modules:
            _to_skip_test_modules[m] = skip_reason(reason)

    #Tests which need hge-key
    if not config.getoption('--hge-key'):
        set_skip_reason('Option --hge-key is missing', 'test_compat.py')

    #Flag --test-webhook-insecure.
    if not config.getoption("--test-webhook-insecure"):
        set_skip_reason('Flag --test-webhook-insecure is missing', 'test_webhook_insecure.py')
    else:
        set_skip_reason(
            'Flag --test-webhook-insecure flag is set',
            all_test_modules_except('test_webhook_insecure.py'))

    #Tests which need jwt configuration
    if any([config.getoption(x) == None for x in ['--hge-jwt-key-file','--hge-jwt-conf']]):
        set_skip_reason('Both --hge-jwt-key-file and --hge-jwt-conf should be present for JWT tests', 'test_jwt.py')

    #Tests with metadata api disabled
    if config.getoption('--test-metadata-disabled'):
        set_skip_reason(
            'Flag --test-metadata-disabled is set',
            all_test_modules_except('test_apis_disabled.py'))

    #Tests with graphql api disabled
    if config.getoption('--test-graphql-disabled'):
        set_skip_reason('Flag --test-graphql-disabled is set', all_graphql_test_modules)

    #Flag --test-cors
    if not config.getoption('--test-cors'):
        set_skip_reason('Flag --test-cors is NOT set', 'test_cors.py')

    #Tests which require a graphql-engine replica
    if not config.getoption('--hge-replica-urls'):
        set_skip_reason('Option --hge-replica-urls is NOT set', 'test_horizontal_scale.py')

    #Flag --test-ws-init-cookie
    if not config.getoption('--test-ws-init-cookie'):
        set_skip_reason('Flag --test-ws-init-cookie is NOT set', 'test_websocket_init_cookie.py')
    else:
        set_skip_reason(
            'Flag --test-ws-init-cookie flag is set',
            all_test_modules_except('test_websocket_init_cookie.py'))

def all_test_modules_except(*args):
    return [f for f in os.listdir() if f.startswith('test_') and f.endswith('.py') and f not in args]

def all_test_modules():
    return get_all_test_modules_except([])

all_graphql_test_modules = ['test_graphql_queries.py', 'test_graphql_mutations.py', 'test_graphql_introspection.py', 'test_subscriptions.py', 'test_schema_stitching.py', 'test_websocket_init_cookie.py']

