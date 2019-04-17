import time
import subprocess
import os
import signal
import json
import dirsync
import shutil
import docker
import threading
import contextlib
from colorama import Fore, Style
from .auth_webhook import auth_handlers
from . import tests_info_db
from .utils import gen_random_password, gen_rsa_key, gen_ca_keys_and_cert, \
    gen_ca_signed_keys_and_cert, get_public_crt, get_private_pem, \
    get_public_pem, get_unused_port, is_graphql_server_running, run_concurrently
from webserver import WebServerProcess
from .test_scenario_conf import hge_scenario_args, hge_scenario_env, scenario_auth_webhook_path, scenario_name
import itertools

class GraphQLServerError(Exception):
    pass

class GraphQLServers:

    default_graphql_env = {
        'HASURA_GRAPHQL_ENABLE_TELEMETRY' : 'false',
        'EVENT_WEBHOOK_HEADER' : "MyEnvValue",
        'HASURA_GRAPHQL_STRINGIFY_NUMERIC_TYPES' : 'true'
    }

    def __init__(self, pg, hge_config, auth_mode, scenario, output_dir, with_replica = False ):
        self.scenario = scenario
        self.output_dir = output_dir
        self.pg = pg
        self.hge_config = hge_config
        self.auth_mode = auth_mode
        self.with_replica = with_replica
        self.set_initial_values()

        os.makedirs(output_dir, exist_ok=True)

        #Use admin secret for all modes except no authorization mode
        if self.auth_mode != 'noAuth':
            self.admin_secret = gen_random_password()

        if self.auth_mode in ['jwt', 'jwtStringified']:
            #Configure JWT
            self.configure_jwt()
        elif self.auth_mode in ['postWebhook','getWebhook']:
            #Configure and start auth webhook
            self.configure_auth_webhook()


    def set_initial_values(self):
        self.admin_secret = None
        self.jwt_key = None
        self.jwt_key_file = None
        self.jwt_conf = None
        self.custom_ca_crts_file = None
        self.auth_webhook_ca_crt_file = None
        self.auth_webhook_root_url = None
        self.hge_processes = []
        self.hge_containers = []
        self.hge_urls = []
        self.evts_webhook_ports = []
        self.hge_replica_urls = []
        self.hge_index = 0
        self.docker_client = None


    def get_scenario_args(self):
        return hge_scenario_args(self.scenario)


    def get_scenario_env(self):
        return hge_scenario_env(self.scenario)


    def configure_jwt(self):
        self.jwt_key = gen_rsa_key()
        self.jwt_key_file = self.output_dir + '/' +  scenario_name(self.scenario) + '_jwt_private.key'
        with open(self.jwt_key_file, 'w') as f:
            f.write(get_private_pem(self.jwt_key))
        self.jwt_conf = {
            'type': 'RS512',
            'key' : get_public_pem(self.jwt_key)
        }
        if self.auth_mode == 'jwtStringified':
            self.jwt_conf['claims_format'] = 'stringified_json'


    #Create a new custom SSL certificates directory and use it for verying SSL certs
    def create_custom_ssl_certs_dir(self):
        self.custom_ssl_certs_dir = os.path.abspath(self.output_dir + "/ssl/certs")
        os.makedirs(self.custom_ssl_certs_dir, exist_ok=True)
        #Copy the default SSL certificates
        with contextlib.redirect_stderr(None):
            dirsync.sync('/etc/ssl/certs', self.custom_ssl_certs_dir, 'sync')

        #Custom SSL CA certificates file
        self.custom_ca_crts_file = self.custom_ssl_certs_dir + "/custom-ca-certificates.crt"
        shutil.copyfile(self.custom_ssl_certs_dir + "/ca-certificates.crt", self.custom_ca_crts_file)

        #Set the CA certificates file for Python requests
        os.environ['REQUESTS_CA_BUNDLE'] = self.custom_ca_crts_file

        return self.custom_ssl_certs_dir


    def configure_auth_webhook(self):
        ssl_certs_dir = self.create_custom_ssl_certs_dir()

        #Generate signed certificates
        (ca_key, ca_cert) = gen_ca_keys_and_cert()
        (webhook_key, webhook_cert) = gen_ca_signed_keys_and_cert(ca_key, ca_cert)

        self.auth_webhook_ca_crt_file = ssl_certs_dir + '/pytest-webhook.crt'
        if self.scenario not in ['webhookInsecure']:
            #Write CA cert into the custom SSL certs directory
            with open(self.auth_webhook_ca_crt_file, 'w') as f:
                f.write(get_public_crt(ca_cert))

            #Add CA to the custom certs bundle
            with open(self.custom_ca_crts_file, 'a') as f:
                f.write(get_public_crt(ca_cert))

        ssl_dir = os.path.abspath(self.output_dir + '/ssl')
        os.makedirs(ssl_dir, exist_ok=True)

        #Create the auth webhook's key and crt files
        self.webhook_key_file = ssl_dir + "/webhook.key"
        with open(self.webhook_key_file, 'w') as f:
            f.write(get_private_pem(webhook_key))

        self.webhook_crt_file = ssl_dir + "/webhook.crt"
        with open(self.webhook_crt_file, 'w') as f:
            f.write(get_public_crt(webhook_cert))


    def auth_webhook_url(self):
        if self.auth_webhook_root_url:
            return self.auth_webhook_root_url + scenario_auth_webhook_path(self.scenario)
        else:
            return None


    def setup(self):
        if self.auth_mode in ['postWebhook','getWebhook']:
            self.run_auth_webhook()

        if self.hge_config == 'withStackExec':
            self.run_graphql_engines_with_executable()
        elif self.hge_config.get('withExecutable'):
            self.run_graphql_engines_with_executable(self.hge_config['withExecutable'])
        elif self.hge_config.get('withDocker'):
            self.run_graphql_engines_with_docker()


    def get_auth_env(self):
        env = dict()

        if self.admin_secret:
            env['HASURA_GRAPHQL_ADMIN_SECRET'] = self.admin_secret

        if self.jwt_key:
            env['HASURA_GRAPHQL_JWT_SECRET'] = json.dumps(self.jwt_conf)
        elif self.auth_webhook_root_url:
            env['HASURA_GRAPHQL_AUTH_HOOK'] = self.auth_webhook_url()
            if self.auth_mode == 'getWebhook':
                env['HASURA_GRAPHQL_AUTH_HOOK_MODE'] = 'GET'
            elif self.auth_mode == 'postWebhook':
                env['HASURA_GRAPHQL_AUTH_HOOK_MODE'] = 'POST'
            env['SYSTEM_CERTIFICATE_PATH'] = self.custom_ssl_certs_dir

        return env


    def run_auth_webhook(self):
        port = get_unused_port(9090)
        server_address = ('127.0.0.1', port)
        log_file = os.path.abspath(self.output_dir + '/' + scenario_name(self.scenario) + '_auth_webhook.log')
        print(Fore.YELLOW, "Running auth webhook on port", port, Style.RESET_ALL)
        self.auth_webhook_process = WebServerProcess(
            auth_handlers,
            ssl_certs=(self.webhook_key_file, self.webhook_crt_file),
            stdout = log_file,
            stderr = log_file,
            server_address = server_address
        )
        self.auth_webhook_process.start()
        tests_info_db.add_reserved_process_port(port, self.auth_webhook_process.pid, "auth webhook")
        self.auth_webhook_root_url = 'https://localhost:' + str(port)


    def stop_auth_webhook(self):
        if self.auth_webhook_root_url:
            print(Fore.YELLOW, "Stopping auth webhook", Style.RESET_ALL)
            self.auth_webhook_process.stop()
            tests_info_db.remove_process_ports(self.auth_webhook_process.pid)
            self.auth_webhook_process.join()


    def get_hge_env(self, db_url, port, tixFile, evts_webhook_port):
        env = {
            **os.environ,
            **self.default_graphql_env,
            'HPCTIXFILE' : tixFile,
            'WEBHOOK_FROM_ENV' : 'http://127.0.0.1:' + str(evts_webhook_port),
            'HASURA_GRAPHQL_DATABASE_URL' : db_url,
            'HASURA_GRAPHQL_SERVER_PORT' : str(port)
        }

        env.update(self.get_auth_env())

        env.update(self.get_scenario_env())

        for k in env:
            if k.startswith('HASURA_GRAPHQL') or 'WEBHOOK' in k:
                print(Fore.BLUE, k, Fore.YELLOW, '=>', Fore.LIGHTGREEN_EX, env[k], Style.RESET_ALL)

        return env


    def get_hge_env_and_args(self, port, db_url, evts_webhook_port):
        self.hge_index += 1
        prefix = self.output_dir + '/grapqh-engine-' + str(self.hge_index) + \
                 '-' + self.auth_mode + '-' + scenario_name(self.scenario)
        log_file = prefix + '.log'
        tix_file = prefix + '.tix'
        tests_info_db.add_hpc_report_file(tix_file)

        hge_env = self.get_hge_env(db_url, port, tix_file, evts_webhook_port)
        return (hge_env, self.get_scenario_args(), log_file)


    def verify_docker_conf(self):
        hge_with_docker = self.hge_config.get('withDocker')

        if not hge_with_docker:
            raise GraphQLServerError("Could not find 'withDocker' config")

        if 'image' not in hge_with_docker:
            raise GraphQLServerError("Hasura GraphQL Engine docker image is not defined within 'withDocker'")

        return hge_with_docker['image']

    def run_graphql_engines_with_docker(self):
        docker_image = self.verify_docker_conf()
        self.docker_client = docker.from_env()
        port = 8080
        evts_webhook_port = 5592

        if self.with_replica:
            hges_conf = itertools.product([False,True], self.pg.db_urls)
        else:
            hges_conf = itertools.product([False], self.pg.db_urls)


        for (i, (replica, db_url)) in enumerate(hges_conf):
            port = get_unused_port(port)
            evts_webhook_port = get_unused_port(evts_webhook_port)
            tests_info_db.add_reserved_process_port(evts_webhook_port, os.getpid(), "pytest events webhook")
            (hge_env, extra_args, log_file) = self.get_hge_env_and_args(port, db_url, evts_webhook_port)
            process_args=['graphql-engine', 'serve', *extra_args]
            docker_ports = {str(port)+'/tcp': ('127.0.0.1', port)}
            volumes = {}
            if self.auth_mode in ['postWebhook', 'getWebhook']:
                volumes[self.custom_ssl_certs_dir] = {
                    'bind': self.custom_ssl_certs_dir,
                    'mode': 'ro'
                }
            print("Running GraphQL Engine docker with image ", Fore.YELLOW, docker_image, Style.RESET_ALL, ", and command: ", Fore.YELLOW, *process_args, Style.RESET_ALL)
            cntnr = self.docker_client.containers.run(
                docker_image,
                command = process_args,
                detach = True,
                ports = docker_ports,
                environment = hge_env,
                network_mode = 'host',
                volumes = volumes
            )
            self.hge_containers.append((cntnr, log_file))
            self.evts_webhook_ports.append(evts_webhook_port)
            tests_info_db.add_reserved_container_port(port,cntnr.name)
            url = 'http://localhost:' + str(port)
            if replica:
                self.hge_replica_urls.append(url)
            else:
                self.hge_urls.append(url)
            port += 1
            evts_webhook_port += 1

        print('Waiting for GraphQl Engine(s) to be up and running.',
              end='', flush=True)
        self.check_if_graphql_processes_are_running(timeout=20)

    def run_graphql_engines_with_executable(self, hge_executable=None):
        port=8080
        evts_webhook_port=5592

        if self.with_replica:
            hges_conf = itertools.product([False, True], self.pg.db_urls)
        else:
            hges_conf = itertools.product([False], self.pg.db_urls)

        for (i, (replica, db_url)) in enumerate(hges_conf):
            port = get_unused_port(port)
            evts_webhook_port = get_unused_port(evts_webhook_port)
            tests_info_db.add_reserved_process_port(evts_webhook_port, os.getpid(), "pytest events webhook")
            (hge_env, extra_args, log_file) = self.get_hge_env_and_args(port, db_url, evts_webhook_port)

            f = open(log_file,'w')
            if hge_executable:
                process_args = [hge_executable, 'serve']
            else:
                process_args = ['stack', 'exec', 'graphql-engine', '--', 'serve']
            process_args.extend(extra_args)
            print('Running GraphQL Engine as: ', Fore.YELLOW, *process_args, Style.RESET_ALL)
            if replica:
                self.check_if_graphql_processes_are_running(timeout=20)
            try:
                p = subprocess.Popen(
                    process_args,
                    stdout=f,
                    stderr=f,
                    env=hge_env
                )
                self.hge_processes.append((p, f))
                tests_info_db.add_reserved_process_port(port, p.pid, 'GraphQL engine')
                url = 'http://localhost:' + str(port)
                if replica:
                    self.hge_replica_urls.append(url)
                else:
                    self.hge_urls.append(url)
                self.evts_webhook_ports.append(evts_webhook_port)
                port += 1
                evts_webhook_port += 1
                time.sleep(0.2)
            except:
                f.close()
                raise
        time.sleep(1)

        print('Waiting for GraphQL Engine(s) to be up and running.',
              end='', flush=True)
        self.check_if_graphql_processes_are_running(timeout=20)


    def check_if_graphql_processes_are_running(self, timeout=60):
        if timeout <=0:
            raise GraphQLServerError("Timeout waiting for graphql processes to start")
        for (p,f) in self.hge_processes:
            if p.poll() != None:
                _file = f.name
                f.close()
                with open(_file) as f:
                    raise GraphQLServerError("GraphQL engine failed with error: " + f.read())
        for (cntnr,f) in self.hge_containers:
            cntnr.reload()
            if cntnr.status == 'exited':
                raise GraphQLServerError("GraphQL engine failed with error: \n"+  cntnr.logs(stdout=True, stderr=True).decode('ascii'))
        for u in self.hge_urls:
            try:
                if not is_graphql_server_running(u):
                    time.sleep(1)
                    print('.', end='', flush=True)
                    return self.check_if_graphql_processes_are_running(timeout-1)
            except Exception as e:
                if timeout < 5:
                    print(repr(e))
                time.sleep(1)
                return self.check_if_graphql_processes_are_running(timeout-1)

    def stop_docker_and_collect_logs(self, cntnr, log_file):
        print(Fore.YELLOW, "Stopping GraphQL Engine docker container ", cntnr.name, Style.RESET_ALL)
        cntnr.stop()

        print(Fore.YELLOW, "Collecting logs from GraphQL Engine docker container ", cntnr.name, Style.RESET_ALL)
        with open(log_file,'wb') as f:
            f.write( cntnr.logs(stdout=True, stderr=True) )

        print(Fore.YELLOW, "Removing GraphQL Engine docker container ", cntnr.name, Style.RESET_ALL)
        cntnr.remove()
        tests_info_db.remove_container_ports(cntnr.name)

    def teardown(self):
        for (proc,log_file) in self.hge_processes:
            log_file.close()
            print(Fore.YELLOW, "Stopping GraphQL engine: pid", proc.pid, Style.RESET_ALL)
            proc.send_signal(signal.SIGINT)
            proc.wait()
            tests_info_db.remove_process_ports(proc.pid)
        self.hge_processes = []
        #Stop docker containers if any
        run_concurrently( [threading.Thread(target=self.stop_docker_and_collect_logs, args=[cntnr, log_file]) for (cntnr, log_file) in self.hge_containers] )

        self.hge_containers = []
        self.hge_urls = []
        self.hge_replica_urls = []
        tests_info_db.release_ports(self.evts_webhook_ports)
        self.evts_webhook_ports = []

        if self.auth_webhook_ca_crt_file:
            try:
                os.remove(self.auth_webhook_ca_crt_file)
                self.auth_webhook_ca_crt_file = None
            except:
                pass
            try:
                os.remove(self.custom_ca_crts_file)
            except:
                pass
        if self.auth_webhook_root_url:
            self.stop_auth_webhook()
            self.auth_webhook_root_url = None
