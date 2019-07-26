import json
import pytest
from colorama import Fore, Style
from .test_conf import pytest_scenario_args, pytest_scenario_default_tests

class PyTestError(Exception):
    pass

class PyTest:

    def __init__(self, pg, hges, scenario, extra_args={}):
        self.extra_args = extra_args
        self.pg = pg
        self.hges = hges
        self.scenario = scenario

    def get_scenario_args(self):
        args = pytest_scenario_args(self.scenario)
        #On flag --test-logging, add the test files
        if all([not x.startswith('test_') for x in self.extra_args]):
            args.extend(pytest_scenario_default_tests(self.scenario))
        print(args)
        if  '--test-logging' in args:
            args.extend(['--hge-log-files', *self.hges.hge_log_files])
        return args

    def run(self):
        parallelism = len(self.pg.db_urls)
        pytest_args = [
            '--hge-urls', *self.hges.hge_urls,
            '--pg-urls', *self.pg.db_urls,
            '--evts-webhook-ports', *[str(x) for x in self.hges.evts_webhook_ports],
            '-rsx'
        ]
        if self.hges.hge_replica_urls and len(self.hges.hge_replica_urls) > 0:
            pytest_args.extend(['--hge-replica-urls', *self.hges.hge_replica_urls])

        if 'no:xdist' not in self.extra_args:
            pytest_args.extend(['-n', str(parallelism)])

        if self.hges.admin_secret:
            #Set admin secret
            pytest_args.extend(['--hge-key', self.hges.admin_secret])
        if self.hges.jwt_key:
            pytest_args.extend([
                '--hge-jwt-key-file',
                self.hges.jwt_key_file,
                '--hge-jwt-conf',
                json.dumps(self.hges.jwt_conf)
            ])
        elif self.hges.auth_webhook_root_url:
            pytest_args.extend(['--hge-webhook', self.hges.auth_webhook_url()])
        pytest_args.extend(self.get_scenario_args())
        pytest_args.append('-v')
        pytest_args.extend(self.extra_args)
        print("Running ", Fore.YELLOW, 'pytest', *pytest_args, Style.RESET_ALL)
        exitStatus = pytest.main(args=pytest_args)
        if exitStatus != 0:
            raise PyTestError("Tests failed: Exit code " + str(exitStatus))
