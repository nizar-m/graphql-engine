import os
import sys
import yaml
import argparse
from test_components import Postgres, GraphQLServers, PyTest, PyTestError
from test_components.test_scenario_conf import  all_auths, all_scenarios,  default_test_conf, scenario_allowed_auths, scenario_name, pg_use_pgbouncer_proxy, with_hge_replica
from colorama import Fore, Style

class TestScenarioError(Exception):
    pass

class TestScenario:

    def verify_config(self, config):
        self.config = config
        self.pg_config = self.config.get('postgres')
        if not self.pg_config:
            raise TestScenarioError("Could not get postgres config")

        self.hge_config = self.config.get("graphqlEngine")
        if not self.pg_config:
            raise TestScenarioError('Could not get config for graphql-engine')

        self.pytest_extra_args = self.config.get('pytest',{}).get('extra_args',[])

        self.auth_mode = self.config.get('auth')
        if not self.auth_mode:
            raise TestScenarioError('Could not get the auth type from config')
        if not self.auth_mode in  all_auths():
            raise TestScenarioError("Unknown auth type " + self.auth_mode)

        self.test_scenario = self.config.get('scenario','default')
        if isinstance(self.test_scenario, str) and not self.test_scenario in all_scenarios():
            raise TestScenarioError('Unknown scenario ' + self.test_scenario)
        if not self.auth_mode in scenario_allowed_auths(self.test_scenario):
            raise TestScenarioError("Authentication type '" + self.auth_mode + "' is not allowed for scenario '" + scenario_name(self.test_scenario) + "'")

    def __init__(self, config):

        self.verify_config(config)

        self.output_dir = os.environ.get('HASURA_TEST_OUTPUT_FOLDER','graphql-engine-test-output')
        print("Tests output folder:", self.output_dir)

        self.pg = Postgres(self.pg_config, self.output_dir, pg_use_pgbouncer_proxy(self.test_scenario))

        self.hges = GraphQLServers(
            self.pg,
            self.hge_config,
            self.auth_mode,
            self.test_scenario,
            self.output_dir,
            with_replica = with_hge_replica(self.test_scenario)
        )

        self.pytest = PyTest(self.pg, self.hges, self.test_scenario, extra_args=self.pytest_extra_args)

        self.hge_index = 1
        self.hge_processes = []
        self.hge_urls = []

    def repeat_with_pgbouncer_restart(self):
        self.pg.restart_pgbouncer_proxies(20)
        self.pytest.run()

    def run(self):
        try:
            self.pg.setup()
            self.hges.setup()
            self.pytest.run()
            if self.test_scenario in ['horizontalScaling']:
                self.repeat_with_pgbouncer_restart()
        except PyTestError as e:
            print (Fore.RED + Style.BRIGHT + repr(e) + Style.RESET_ALL)
            sys.exit(1)
        finally:
            self.teardown()
        print (Fore.GREEN + Style.BRIGHT + '\nPASSED ' + Fore.YELLOW  + 'scenario: ' + Fore.BLUE + scenario_name(self.test_scenario) + Fore.YELLOW + ', auth: ' + Fore.BLUE + self.auth_mode + Style.RESET_ALL + '\n')

    def teardown(self):
        self.hges.teardown()
        self.pg.teardown()

def _first_true(iterable, default=False, pred=None):
    return next(filter(pred, iterable), default)

class TestCases:

    def parse_args(self):
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('-f', metavar='FILE', help='Test configuration file', required=False)
        arg_parser.add_argument('--pg-urls', metavar='HASURA_TEST_PG_URLS', help='Postgres database urls to be used for tests', required=False)
        arg_parser.add_argument('--pg-docker-image', metavar='HASURA_TEST_PG_DOCKER_IMAGE', help='Docker image to be used for tests', required=False)
        arg_parser.add_argument('--hge-docker-image', metavar='HASURA_TEST_HGE_DOCKER_IMAGE', help='Docker image to be used for tests', required=False)
        arg_parser.add_argument('--hge-executable', metavar='HASURA_TEST_GRAPHQL_ENGINE', help='GraphQL engine executable to be used for tests', required=False)
        arg_parser.add_argument('--scenario', metavar='HASURA_TEST_SCENARIO', help='Scenario for which the tests should be run', required=False)
        arg_parser.add_argument('--auth-mode', metavar='HASURA_TEST_AUTH_MODE', help='Authentication mode to be used for tests', required=False)
        arg_parser.add_argument('pytest_args', nargs=argparse.REMAINDER)
        self.parsed_args = arg_parser.parse_args()
        self.pytest_args = self.parsed_args.pytest_args[1:]
        file_conf = self.parsed_args.f
        conf_from_args =  any([getattr(self.parsed_args,x) for x in ['pg_urls', 'pg_docker_image', 'hge_docker_image', 'scenario']] )
        if file_conf and conf_from_args:
            print("When a configuration file (-f FILE) is provided, the other options '--pg-urls', '--pg-docker-image', '--hge-docker-image', '--hge-executable' and 'scenario' should not be specified")
        return self.parsed_args

    def get_param(self, attr, env_var):
        return _first_true([getattr(self.parsed_args, attr), os.getenv(env_var)])

    def get_test_conf(self):
        conf_from_file = self.read_conf_from_file()
        if conf_from_file:
            return conf_from_file
        else:
            return self.conf_from_args()

    def conf_from_args(self):
        conf = default_test_conf()
        args = self.parsed_args
        if args.pg_urls and args.pg_docker_image:
            print('--pg-urls and --pg-docker-image should not be defined together')
            sys.exit(1)

        pg_docker_image = self.get_param('pg_docker_image', 'HASURA_TEST_PG_DOCKER_IMAGE')
        if pg_docker_image:
            if not conf.get('postgres',{}).get('withDocker'):
                conf['postgres'] = default_test_conf()['postgres']
            conf['postgres']['withDocker']['image'] = pg_docker_image

        pg_urls = self.get_param('pg_urls', 'HASURA_TEST_PG_URLS')
        if pg_urls:
            conf['postgres'] = {
                'urls' : pg_urls.split(',')
            }

        hge_docker_image =  self.get_param('hge_docker_image', 'HASURA_TEST_HGE_DOCKER_IMAGE')
        if hge_docker_image:
            conf['graphqlEngine'] = {
                'withDocker' : {
                    'image': hge_docker_image
                }
            }

        hge_executable = self.get_param('hge_executable', 'HASURA_TEST_GRAPHQL_ENGINE')
        if hge_executable:
            conf['graphqlEngine'] = {
                'withExecutable' : hge_executable
            }

        scenario = self.get_param('scenario', 'HASURA_TEST_SCENARIO')
        if scenario:
            conf['scenario'] = scenario

        auth = self.get_param('auth_mode', 'HASURA_TEST_AUTH_MODE')
        if auth:
            conf['auth'] = auth

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

    def __init__(self):
        self.pytest_args = []
        self.test_cases = []
        self.get_test_cases()

    def add_pytest_args(self, conf):
        if self.pytest_args:
            if not conf.get('pytest'):
                conf['pytest'] = {'extra_args': self.pytest_args}
            elif not conf.get('pytest',{}).get('extra_args'):
                conf['pytest']['extra_args']  = self.pytest_args
            else:
                conf['pytest']['extra_args'].extend(self.pytest_args)

    def extract_test_cases(self, conf):
        if isinstance(conf, list):
            for c in conf:
                #Consider each one as a different test case
                self.extract_test_cases(conf)
        if isinstance(conf, dict):
            auths = conf.get('auth')
            scenarios = conf.get('scenario')
            if isinstance(auths, list):
                for auth in auths:
                    cur_conf = conf.copy()
                    cur_conf['auth'] = auth
                    #Each auth as a different case
                    self.extract_test_cases(cur_conf)
            elif isinstance(scenarios, list):
                for scenario in scenarios:
                    cur_conf = conf.copy()
                    cur_conf['scenario'] = scenario
                    #Each scenario as a different case
                    self.extract_test_cases(cur_conf)
            else:
                if isinstance(conf['scenario'], dict) and conf['scenario'].get('hgeReplica'):
                    conf['scenario']['withHgeReplicas'] = True
                self.add_pytest_args(conf)
                self.test_cases.append(conf)

    def run(self):
        for test_case in self.test_cases:
            print(test_case)
            t = TestScenario(config = test_case)
            t.run()

if __name__ == '__main__':
    TestCases().run()
