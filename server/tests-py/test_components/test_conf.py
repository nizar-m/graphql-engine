import yaml
import os

_defined_scenarios = None
_auths_conf = None
_default_test_conf = None

class ScenarioError(Exception):
    pass


def _conf_dir():
    def parent_dir(f):
        dir = os.path.dirname
        return dir(dir(f))
    return parent_dir(__file__) + '/test_config'


def _get_conf(conf_file):
    conf_file = _conf_dir() + '/' + conf_file
    with open(conf_file) as f:
        return yaml.safe_load(f.read())


def _get_confs():
    global _auths_conf, _defined_scenarios, _default_test_conf
    _auths_conf = _get_conf('auth_conf.yaml')
    _defined_scenarios = _get_conf('scenario_conf.yaml')
    _default_test_conf = _get_conf('default_test_conf.yaml')


_get_confs()


def default_test_conf():
    return _default_test_conf


def get_auth_conf_defs(auth_type):
    if auth_type not in _auths_conf:
        raise ScenarioError('Unknown auth ' + auth_type)
    return _auths_conf[auth_type]


def validate_auth_conf(conf_key, conf_value, auth_conf_def):
    conf_type = auth_conf_def['type']
    type_map = {
        'string'  : str,
        'list'    : list,
        'boolean' : bool
    }
    if isinstance(conf_type, str):
        allowed_types = [type_map[conf_type]]
    else:
        allowed_types = [type_map[ty] for ty in conf_type]
    #Validate the type of the configuration
    if all([not isinstance(conf_value, ty) for ty in allowed_types]):
        raise ScenarioError('Config "' + conf_key + '" should be of type "' + conf_type + '"')
    allowed_vals = auth_conf_def.get('allowedValues')
    #Check whether the value is among the allowed values
    if allowed_vals and not conf_value in allowed_vals:
        raise ScenarioError('Allowed values for ' + conf_key + ' are ' + str(allowed_vals))


def validate_auth(auth):
    if isinstance(auth, str):
        return validate_auth({auth: {}})
    else:
        assert len(auth.keys()) == 1
        auth_type = list(auth.keys())[0]
        auth_conf = auth[auth_type]
        conf_defs = get_auth_conf_defs(auth_type)
        for (conf_key, conf_def) in conf_defs.items():
            if auth_conf.get(conf_key) is not None:
                conf_val = auth_conf[conf_key]
                validate_auth_conf(conf_key, conf_val, conf_def)
            else:
                #Set default value for the configuration
                auth_conf[conf_key] = conf_def['default']
    return auth


def all_defined_scenarios():
    return list(_defined_scenarios.keys())


def scenario_name(scenario):
    if isinstance(scenario, str):
        return scenario
    else:
        return scenario.get('name', 'custom')


def auth_type(scenario):
    return list(scenario_auth(scenario).keys())[0]


def scenario_conf(scenario, path, default):
    if isinstance(scenario, str):
        return _dict_get(_defined_scenarios[scenario], path, default)
    else:
        return _dict_get(scenario, path, default)


def validate_scenario(scenario):
    if isinstance(scenario, str):
        if not scenario in all_defined_scenarios():
            raise ScenarioError('Unknown scenario ' + scenario)
        name = scenario
        scenario = _defined_scenarios[scenario]
        scenario.update({'name': name})
    if scenario.get('hgeReplica'):
        scenario['withHgeReplicas'] = True
    validate_auth(scenario_auth(scenario))
    return scenario


def pg_use_pgbouncer_proxy(scenario):
    return scenario_conf(scenario, ['postgres', 'withPgbouncerProxy'], False)


def scenario_auth_or_none(scenario):
    return scenario_conf(scenario, ['auth'], None)


def scenario_auth(scenario):
    def_auth = { 'noAuth': {}}
    return scenario_conf(scenario, ['auth'], def_auth)


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


def scenario_auth_webhook_path(scenario):
    return scenario_conf(scenario, ['authWebhookPath'], '/token-as-base64-of-headers')


def _dict_get(d,p,default=None):
    if len(p) == 0:
        return d
    elif len(p) == 1:
        return d.get(p[0], default)
    else:
        return _dict_get(d.get(p[0], {}), p[1:], default)
