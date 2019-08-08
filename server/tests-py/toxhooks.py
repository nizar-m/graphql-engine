import pluggy
from packaging.version import Version

hookimpl = pluggy.HookimplMarker("tox")

def _set_pg_docker_image(env_conf):
    if env_conf.pgdockerimage:
        for f in [x for x in env_conf.factors if x.startswith('pgDocker')]:
            pg_docker_image = env_conf.pgdockerimage % (f.strip('pgDocker'),)
            env_conf.setenv['HASURA_TEST_PG_DOCKER_IMAGE'] = pg_docker_image


def _set_hge_beta_docker_image(env_conf):
    if env_conf.hgebetadockerimage:
        for f in [x for x in env_conf.factors if x.startswith('hgeDocker')]:
            hge_docker_image = env_conf.hgebetadockerimage % (f.strip('hgeDockerBeta',))
            env_conf.setenv['HASURA_TEST_HGE_DOCKER_IMAGE'] = hge_docker_image


def _is_scenario_factor(factor):
    if any([factor.startswith(x) for x in ('pgDocker','hgeDocker','py')]):
        return False
    if factor.endswith('Auth'):
        return False
    if any([factor == x for x in ['pgUrl','hgeExec','hgeStackExec'] ]):
        return False
    return True


def _set_scenario(env_conf):
    if env_conf.scenario:
        for f in [ x for x in env_conf.factors if _is_scenario_factor(x) ]:
            env_conf.setenv['HASURA_TEST_SCENARIO'] = env_conf.scenario % f


def _set_auth(env_conf):
    if env_conf.auth:
        env_conf.setenv['HASURA_TEST_AUTH'] = env_conf.auth.replace('\{','{').replace('\}','}')


@hookimpl
def tox_addoption(parser):
    parser.add_testenv_attribute(
        "pgdockerimage",
        type="string",
        help="Docker image to be used for Postgres"
    )
    parser.add_testenv_attribute(
        "hgebetadockerimage",
        type="string",
        help="Docker image to be used for GraphQL Engine (Beta)"
    )
    parser.add_testenv_attribute(
        "scenario",
        type="string",
        help="Test scenario"
    )
    parser.add_testenv_attribute(
        "auth",
        type="string",
        help="Authentication type to be used for tests"
    )


@hookimpl
def tox_configure(config):
    for env_conf in config.envconfigs.values():
        _set_pg_docker_image(env_conf)
        _set_hge_beta_docker_image(env_conf)
        _set_scenario(env_conf)
        _set_auth(env_conf)
