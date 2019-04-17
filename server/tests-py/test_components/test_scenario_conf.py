import yaml
import os

_scenario_conf = None

def _get_conf():
    global _scenario_conf
    if _scenario_conf:
        return
    conf_file = os.path.dirname(os.path.abspath(__file__)) + '/test_scenario_conf.yaml'
    with open(conf_file) as f:
        _scenario_conf = yaml.safe_load(f.read())

_get_conf()

def default_test_conf():
    return _scenario_conf['defaultTestConf']

def all_auths():
    return _scenario_conf['auth']

def all_scenarios():
    return list(_scenario_conf['scenario'].keys())

def scenario_name(scenario):
    if isinstance(scenario, str):
        return scenario
    else:
        return scenario.get('name','custom')

def scenario_conf(scenario, path, default):
    if isinstance(scenario, str):
        return _dict_get(_scenario_conf['scenario'][scenario], path, default)
    else:
        return _dict_get(scenario, path, default)

def pg_use_pgbouncer_proxy(scenario):
    return scenario_conf(scenario, ['postgres', 'withPgbouncerProxy'], False)

def hge_scenario_env(scenario):
    return scenario_conf(scenario, ['hge', 'env'], {})

def hge_scenario_args(scenario):
    return scenario_conf(scenario, ['hge', 'args'], [])

def with_hge_replica(scenario):
    return scenario_conf(scenario, ['withHgeReplica'], False)

def pytest_scenario_args(scenario):
    return scenario_conf(scenario, ['pytest', 'args'], [])

def pytest_scenario_default_tests(scenario):
    return scenario_conf(scenario, ['pytest', 'defaultTests'], [])

def scenario_allowed_auths(scenario):
    return scenario_conf(scenario, ['allowedAuth'], all_auths())

def scenario_auth_webhook_path(scenario):
    return scenario_conf(scenario, ['authWebhookPath'], '/token-as-base64-of-headers')

def _dict_get(d,p,default=None):
    if len(p) == 0:
        return d
    elif len(p) == 1:
        return d.get(p[0], default)
    else:
        return _dict_get(d.get(p[0], {}), p[1:], default)
