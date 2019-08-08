import docker
import time
import tempfile
from .utils import get_unused_port, gen_random_password, pg_create_database, run_sql, run_concurrently
from .pgbouncer import PGBouncer
import os
from colorama import Fore, Style
from . import tests_info_db
import threading

class PostgresError(Exception):
    pass

class Postgres:
    def __init__(self, pg_config, output_dir, use_pgbouncer_proxy=False):
        self.pg_config = pg_config
        self.db_urls = []
        self.pgb_proxies = []
        self.pg_container = None
        self.docker_client = None
        self.output_dir = output_dir
        self.use_pgbouncer_proxy = use_pgbouncer_proxy

    def setup(self):
        if self.pg_config.get('withDocker'):
            self.setup_postgres_docker()
        elif self.pg_config.get('urls'):
            self.db_urls = self.pg_config['urls']
        else:
            PostgresError("Either postgres 'urls' or 'withDocker' should be provided")
        if self.use_pgbouncer_proxy:
           self.proxy_via_pgbouncer()

    def start_postgres_docker(self, docker_image, database):
        self.port = get_unused_port(5432)
        self.user = 'hge_test'
        self.password = gen_random_password()
        env = {
            'POSTGRES_USER' : self.user,
            'POSTGRES_PASSWORD' : self.password,
            'POSTGRES_DB' : database
        }
        docker_ports = {'5432/tcp': ('127.0.0.1', self.port)}
        self.docker_client = docker.from_env()
        print("Running postgres docker with image: " + docker_image)
        cntnr = self.docker_client.containers.run(
            docker_image,
            detach=True,
            ports=docker_ports,
            environment=env
        )
        self.pg_container = cntnr
        pg_url = 'postgresql://' + self.user + ':' + self.password + '@localhost:' + str(self.port) + '/' + database
        print("Waiting for database to be up and running.", end="", flush=True)
        self.wait_for_db_start(pg_url, timeout=30)
        tests_info_db.add_reserved_container_port(self.port, self.pg_container.name, "Postgres")
        print("")
        return (cntnr, pg_url)

    def proxy_via_pgbouncer(self):
        port = get_unused_port(6543)

        db_urls = self.db_urls.copy()
        self.db_urls = set()

        for i,db_url in enumerate(db_urls):
            conf_dir = self.output_dir + "/pgbouncer_" + str(port)
            if os.getuid() == 0:
                conf_dir_orig = conf_dir
                conf_dir = tempfile.gettempdir() + '/pgbouncer_' + str(port)
                try:
                    os.remove(conf_dir_orig)
                    os.symlink(conf_dir, conf_dir_orig)
                except Exception:
                    pass
            pgb_proxy = PGBouncer(db_url, conf_dir, port)
            pgb_proxy.start()
            print(Fore.YELLOW, "pgbouncer proxy:", Fore.BLUE + db_url,
                  Fore.YELLOW + '=>', Fore.GREEN + pgb_proxy.get_pgbouncer_url() + Style.RESET_ALL)
            self.db_urls.add(pgb_proxy.get_pgbouncer_url())
            self.pgb_proxies.append(pgb_proxy)
            port = get_unused_port(port+1)

        for url in self.db_urls:
            self.wait_for_db_start(url, timeout=30)


    def verify_postgres_docker_conf(self):
        pg_with_docker = self.pg_config.get('withDocker')

        if not pg_with_docker:
           raise PostgresError("Could not find 'withDocker' config")

        if 'image' not in pg_with_docker:
           raise PostgresError("Postgres docker image is not defined")
        docker_image = pg_with_docker['image']

        if 'databases' not in pg_with_docker:
            raise PostgresError("List of databases to run tests on is not defined")
        dbs = pg_with_docker['databases']
        if len(dbs) <= 0:
            raise PostgresError("Atleast name of one database should be defined")

        return(docker_image,dbs)

    def setup_postgres_docker(self):
        (docker_image, dbs) = self.verify_postgres_docker_conf()
        (cntnr, pg_url) = self.start_postgres_docker(docker_image, dbs[0])
        dburls = [pg_url]
        for db in dbs[1:]:
            pg_create_database(pg_url, db, exists_ok = True)
            cur_pg_url = 'postgresql://' + self.user + ':' + self.password + '@localhost:' + str(self.port) + '/' + db
            dburls.append(cur_pg_url)
        self.db_urls = dburls

    def wait_for_db_start(self, db_url, timeout=60):
        if timeout > 0:
            try:
                run_sql(db_url, 'select 1')
                return
            except Exception as e:
                if timeout < 5:
                    print("\nWaiting for database to be up and running:" + repr(e), end=""),
                else:
                    print(".", end="", flush=True),
                time.sleep(1)
                self.wait_for_db_start(db_url, timeout-1)
        else:
            raise PostgresError("Timeout waiting for database to start")

    def teardown(self):
        self.stop_docker_containers()
        self.stop_pgbouncer_proxies()

    def stop_docker_containers(self):
        if self.pg_container:
            cntnr_info = "Postgres docker container " + self.pg_container.name + " " + repr(self.pg_container.image)
            print(Fore.YELLOW + "Stopping " + cntnr_info + Style.RESET_ALL)
            self.pg_container.stop()
            print(Fore.YELLOW + "Removing " + cntnr_info + Style.RESET_ALL)
            self.pg_container.remove()
            tests_info_db.remove_container_ports(self.pg_container.name)

    def stop_pgbouncer_proxies(self):
        for pgb_proxy in self.pgb_proxies:
            pgb_proxy.stop()

    def restart_pgbouncer_proxies(self, delay_after_stop=1):
        run_concurrently([
            threading.Thread(target=pgb_proxy.restart, args=[delay_after_stop])
            for pgb_proxy in self.pgb_proxies])
