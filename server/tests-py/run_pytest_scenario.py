import os
import sys
import hashlib
import yaml
import argparse
import json
from test_components import Postgres, GraphQLServers, PyTest, PyTestError
from test_components.test_conf import validate_scenario, auth_type, pg_use_pgbouncer_proxy, with_hge_replica, scenario_name, default_test_conf, scenario_auth_or_none, validate_auth
from test_components.tests_info_db import output_dir
from colorama import Fore, Style
import logging

class TestScenarioError(Exception):
    pass


class TestScenario:

    def __init__(self, config):

        self.verify_config(config)

        self.output_dir = output_dir()
        print("Tests output folder:", self.output_dir)

        self.pg = Postgres(self.pg_config, self.output_dir, pg_use_pgbouncer_proxy(self.test_scenario))

        self.hges = GraphQLServers(
            self.pg,
            self.hge_config,
            self.test_scenario,
            self.output_dir,
            self.conf_hash,
            with_replica = with_hge_replica(self.test_scenario)
        )

        self.pytest = PyTest(self.pg, self.hges, self.test_scenario, extra_args=self.pytest_extra_args)

        self.hge_index = 1
        self.hge_processes = []
        self.hge_urls = []


    def verify_config(self, config):
        self.config = config
        self.conf_hash = hashlib.sha256(json.dumps(config, separators=(',', ':'), sort_keys=True).encode('utf-8')).hexdigest()[:8]
        self.pg_config = self.config.get('postgres')
        if not self.pg_config:
            raise TestScenarioError("Could not get postgres config")

        self.hge_config = self.config.get("graphqlEngine")
        if not self.hge_config:
            raise TestScenarioError('Could not get config for graphql-engine')

        self.pytest_extra_args = self.config.get('pytest',{}).get('extraArgs',[])

        test_scenario = self.config.get('scenario', 'default')
        self.test_scenario = validate_scenario(test_scenario)


    def run(self):
        try:
            os.environ['HASURA_TEST_SUCCESS'] = 'false'
            self.pg.setup()
            self.hges.run()
            self.pytest.run()
            if scenario_name(self.test_scenario) in ['horizontalScaling']:
                self.repeat_with_pgbouncer_restart()
            os.environ['HASURA_TEST_SUCCESS'] = 'true'
        except PyTestError as e:
            print (Fore.RED + Style.BRIGHT + repr(e) + Style.RESET_ALL)
            sys.exit(1)
        finally:
            self.teardown()
        print (Fore.GREEN + Style.BRIGHT + '\nPASSED ' + Fore.YELLOW  + 'scenario: ' + Fore.BLUE + scenario_name(self.test_scenario) + Fore.YELLOW + ', auth: ' + Fore.BLUE + auth_type(self.test_scenario) + Style.RESET_ALL + '\n')


    def teardown(self):
        self.hges.teardown()
        self.pg.teardown()


    def repeat_with_pgbouncer_restart(self):
        self.pg.restart_pgbouncer_proxies(20)
        self.pytest.run()


class TestCases:


    def __init__(self):
        self.set_arg_parse_options()
        self.pytest_args = []
        self.test_cases = []
        self.set_log_level()
        self.get_test_cases()

    def set_log_level(self):
        LOGLEVEL = os.environ.get('HASURA_TEST_LOGLEVEL','WARNING')
        logging.basicConfig(level=LOGLEVEL)

    def set_pg_options(self):
        self.arg_parser.add_argument('--pg-urls', metavar='HASURA_TEST_PG_URLS', help='Postgres database urls to be used for tests', required=False)
        self.arg_parser.add_argument('--pg-docker-image', metavar='HASURA_TEST_PG_DOCKER_IMAGE', help='Postgres docker image to be used for tests', required=False)


    def set_hge_options(self):
        self.arg_parser.add_argument('--hge-docker-image', metavar='HASURA_TEST_HGE_DOCKER_IMAGE', help='GraphQl engine docker image to be used for tests', required=False)
        self.arg_parser.add_argument('--hge-executable', metavar='HASURA_TEST_GRAPHQL_ENGINE', help='GraphQL engine executable to be used for tests', required=False)
        self.arg_parser.add_argument('--hge-rts-opts', metavar='HASURA_TEST_HGE_RTS_OPTS', help='rts opts for GraphQL engine', required=False)


    def set_scenario_options(self):
        self.arg_parser.add_argument('--scenario', metavar='HASURA_TEST_SCENARIO', help='Scenario for which the tests should be run', required=False)

    def set_auth_options(self):
        self.arg_parser.add_argument('--auth', metavar='HASURA_TEST_AUTH', help='Authentication mode to be used for GraphQL Engine', required=False)


    def set_pytest_args_option(self):
        self.arg_parser.add_argument('pytest_args', nargs=argparse.REMAINDER)


    def set_arg_parse_options(self):
        self.arg_parser = argparse.ArgumentParser()
        self.arg_parser.add_argument('-f', metavar='FILE', help='Test configuration file. (The other options given below should not be used along with this argument. Those options may conflict with the configuration file).', required=False)
        self.set_pg_options()
        self.set_hge_options()
        self.set_auth_options()
        self.set_scenario_options()
        self.set_pytest_args_option()

    def parse_args(self):
        self.parsed_args = self.arg_parser.parse_args()
        self.pytest_args = self.parsed_args.pytest_args[1:]
        file_conf = self.parsed_args.f
        conf_from_args =  any([getattr(self.parsed_args,x) for x in ['pg_urls', 'pg_docker_image', 'hge_docker_image', 'scenario']] )
        if file_conf and conf_from_args:
            print("When a configuration file (-f FILE) is provided, the other options '--pg-urls', '--pg-docker-image', '--hge-docker-image', '--hge-executable' and 'scenario' should not be specified")
        return self.parsed_args


    def get_param(self, attr, env):
        return _first_true([getattr(self.parsed_args, attr), os.getenv(env)])


    def get_exclusive_params(self, params_loc):
        excl_param = None
        params_out = []
        for (attr, env) in params_loc:
            param = self.get_param(attr, env)
            params_out.append(param)
            if param:
                if not excl_param:
                    excl_param = (param, attr, env)
                else:
                    (param1, attr1, env1) = excl_param
                    def loc(a, e):
                        arg = '--' + a.replace('_','-')
                        return arg + '(env: ' + e + ')'
                    print(loc(attr, env), 'and', loc(attr1, env1), 'should not be defined together')
                    sys.exit(1)
        return params_out


    def get_test_conf(self):
        conf_from_file = self.read_conf_from_file()
        if conf_from_file:
            return conf_from_file
        else:
            return self.conf_from_args()


    def set_pg_conf_from_args(self, conf):

        pg_urls, pg_docker_image = self.get_exclusive_params([
            ('pg_urls', 'HASURA_TEST_PG_URLS'),
            ('pg_docker_image', 'HASURA_TEST_PG_DOCKER_IMAGE')
        ])

        if pg_urls:
            conf['postgres'] = {
                'urls' : pg_urls.split(',')
            }

        if pg_docker_image:
            if not conf.get('postgres',{}).get('withDocker'):
                conf['postgres'] = default_test_conf()['postgres']
            conf['postgres']['withDocker']['image'] = pg_docker_image


    def set_hge_conf_from_args(self, conf):

        docker_image, executable = self.get_exclusive_params([
            ('hge_docker_image', 'HASURA_TEST_HGE_DOCKER_IMAGE'),
            ('hge_executable', 'HASURA_TEST_GRAPHQL_ENGINE')
        ])

        if docker_image:
            conf['graphqlEngine'] = {
                'withDocker' : { 'image': docker_image }
            }

        if executable:
            conf['graphqlEngine'] = {
                'withExecutable' : executable
            }

        rts_opts = self.get_param('hge_rts_opts', 'HASURA_TEST_HGE_RTS_OPTS')
        print("DEBUG RTS OPTS", rts_opts)
        if rts_opts:
            conf['graphqlEngine']['rtsOpts'] = rts_opts.split(" ")


    def set_scenario_from_args(self, conf):
        scenario = self.get_param('scenario', 'HASURA_TEST_SCENARIO') or 'default'
        if ':' in scenario:
            scenario = yaml.safe_load(scenario)
        conf['scenario'] = validate_scenario(scenario)


    def set_auth_conf_from_args(self, conf):
        scenario = conf['scenario']
        scenario_auth = scenario_auth_or_none(scenario)
        if scenario_auth:
            return

        auth_conf = self.get_param('auth', 'HASURA_TEST_AUTH')

        if auth_conf:
            if ':' in auth_conf:
                auth_conf = yaml.safe_load(auth_conf)
            scenario['auth'] = validate_auth(auth_conf)


    def conf_from_args(self):
        conf = default_test_conf()
        self.set_pg_conf_from_args(conf)
        self.set_hge_conf_from_args(conf)
        self.set_scenario_from_args(conf)
        self.set_auth_conf_from_args(conf)
        return conf


    def read_conf_from_file(self):
        conf_file = self.parsed_args.f
        if not conf_file:
            return None
        elif conf_file == '-':
            conf = sys.stdin.read()
        else:
            with open(conf_file,'r') as f:
                conf = f.read()
        return yaml.safe_load(conf)


    def get_test_cases(self):
        self.parse_args()
        conf = self.get_test_conf()
        self.extract_test_cases(conf)


    def add_pytest_args(self, conf):
        if self.pytest_args:
            if not conf.get('pytest'):
                conf['pytest'] = {'extraArgs': self.pytest_args}
            elif not conf.get('pytest',{}).get('extraArgs'):
                conf['pytest']['extraArgs']  = self.pytest_args
            else:
                conf['pytest']['extraArgs'].extend(self.pytest_args)


    def extract_test_cases(self, conf):
        if isinstance(conf, list):
            for c in conf:
                #Consider each one as a different test case
                self.extract_test_cases(conf)
        elif isinstance(conf, dict):
            scenarios = conf.get('scenario')
            if isinstance(scenarios, list):
                #Consider each scenario as a different test case
                for scenario in scenarios:
                    cur_conf = conf.copy()
                    cur_conf['scenario'] = scenario
                    self.extract_test_cases(cur_conf)
            else:
                self.add_pytest_args(conf)
                self.test_cases.append(conf)


    def run(self):
        for test_case in self.test_cases:
            print (Fore.YELLOW)
            print(yaml.dump(test_case) + Style.RESET_ALL)
            t = TestScenario(config = test_case)
            t.run()


def _first_true(iterable, default=False, pred=None):
    return next(filter(pred, iterable), default)


if __name__ == '__main__':
    TestCases().run()
