import os


_to_skip_test_modules = dict()


def skip_module(filename):
    return _to_skip_test_modules.get(os.path.basename(filename))


def set_skip_test_module_rules(config):
    skip_if_no_admin_secret(config)
    skip_with_insecure_webhook_flag(config)
    skip_if_no_jwt_conf(config)
    skip_if_metadata_api_disabled(config)
    skip_if_graphql_api_disabled(config)
    skip_with_test_cors_flag(config)
    skip_if_no_hge_replicas(config)
    skip_with_ws_init_cookie_flag(config)
    skip_with_test_logging_flag(config)
    skip_if_no_hge_log_option(config)
    skip_if_no_allowlist_flag(config)


def skip_if_no_admin_secret(config):
    #Tests which need hge-key
    if not config.getoption('--hge-key'):
        set_skip_reason('Option --hge-key is NOT set', 'test_compat.py')


def skip_with_insecure_webhook_flag(config):
    #Flag --test-webhook-insecure.
    if not config.getoption("--test-webhook-insecure"):
        set_skip_reason('Flag --test-webhook-insecure is NOT set', 'test_webhook_insecure.py')
    else:
        set_skip_reason(
            'Flag --test-webhook-insecure flag is set',
            all_test_modules_except('test_webhook_insecure.py'))


def skip_if_no_jwt_conf(config):
    #Tests which need jwt configuration
    if any([config.getoption(x) == None for x in ['--hge-jwt-key-file','--hge-jwt-conf']]):
        set_skip_reason('Both --hge-jwt-key-file and --hge-jwt-conf should be present for JWT tests', 'test_jwt.py')


def skip_if_metadata_api_disabled(config):
    #Tests with metadata api disabled
    if config.getoption('--test-metadata-disabled'):
        set_skip_reason(
            'Flag --test-metadata-disabled is set',
            all_test_modules_except('test_apis_disabled.py'))


def skip_if_graphql_api_disabled(config):
    #Tests with graphql api disabled
    if config.getoption('--test-graphql-disabled'):
        set_skip_reason('Flag --test-graphql-disabled is set', all_graphql_test_modules)


#Flag --test-cors
def skip_with_test_cors_flag(config):
    if not config.getoption('--test-cors'):
        set_skip_reason('Flag --test-cors is NOT set', 'test_cors.py')


#Tests which require a graphql-engine replica
def skip_if_no_hge_replicas(config):
    if not config.getoption('--hge-replica-urls'):
        set_skip_reason('Option --hge-replica-urls is NOT set', 'test_horizontal_scale.py')


#Flag --test-ws-init-cookie
def skip_with_ws_init_cookie_flag(config):
    if not config.getoption('--test-ws-init-cookie'):
        set_skip_reason('Flag --test-ws-init-cookie is NOT set', 'test_websocket_init_cookie.py')
    else:
        set_skip_reason(
            'Flag --test-ws-init-cookie flag is set',
            all_test_modules_except('test_websocket_init_cookie.py'))

#Flag --test-logging
def skip_with_test_logging_flag(config):
    if not config.getoption('--test-logging'):
        set_skip_reason('Flag --test-logging is NOT set', 'test_logging.py')

def skip_if_no_hge_log_option(config):
    #Option --hge-log-files
    if not config.getoption('--hge-log-files'):
        set_skip_reason('Option --hge-log-files is NOT set', 'test_logging.py')

def skip_if_no_allowlist_flag(config):
    #Option --test-allowlist-queries
    if not config.getoption('--test-allowlist-queries'):
        set_skip_reason('Flag --test-allowlist-queries is NOT set', 'test_allowlist_queries.py')

def skip_reason(s):
        return s + ". Skipping tests in this module"


def set_skip_reason(reason, modules):
    if isinstance(modules, str):
        return set_skip_reason(reason, [modules])
    for m in modules:
        _to_skip_test_modules[m] = skip_reason(reason)


def all_test_modules_except(*args):
    return [f for f in os.listdir() if f.startswith('test_') and f.endswith('.py') and f not in args]


def all_test_modules():
    return all_test_modules_except([])


all_graphql_test_modules = ['test_graphql_queries.py', 'test_graphql_mutations.py', 'test_graphql_introspection.py', 'test_subscriptions.py', 'test_schema_stitching.py', 'test_websocket_init_cookie.py']
