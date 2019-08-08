import shutil
from urllib.parse import urlparse
from .utils import run_sql, gen_random_password, popen_user
from . import tests_info_db
import subprocess
import time
import os
import pwd
import psycopg2
from colorama import Fore, Style
from pathlib import Path


class PGBouncerError(Exception):
    pass

class PGBouncer:

    def verify_pgbouncer_exists(self):
        if not shutil.which("pgbouncer"):
            raise PGBouncerError("pgbouncer is not installed")

    def verify_db_url(self, db_url):
        parsed = urlparse(db_url)

        self.db_username = parsed.username
        self.db_password = parsed.password
        self.db_hostname = parsed.hostname
        self.db_port = parsed.port

        if not (self.db_username and self.db_hostname and self.db_port):
            raise PGBouncerError("Failure parsing db_url")

        dbname_resp = run_sql(db_url, 'select current_database() as database')
        self.db_database = dbname_resp.fetchone()[0]


    def __init__(self, db_url, conf_dir, port=6543):
        self.verify_pgbouncer_exists()
        self.verify_db_url(db_url)
        self.user = 'postgres'
        self.password = gen_random_password()
        self.database = 'pgb_hge_test'
        self.conf_dir = os.path.abspath(conf_dir)
        self.port = port
        self.ini_file = self.conf_dir + "/pgbouncer.ini"
        self.auth_file = self.conf_dir + "/users.txt"
        self.log_file = self.conf_dir + "/pgbouncer.log"
        self.pid_file = self.conf_dir + "/pgbouncer.pid"
        self.process = None

    def create_ini_file(self):
        with open(self.ini_file,'w') as f:
            f.write('''
[databases]
{pgb_database} = host={host} port={port} user={user} password={password} dbname={database}

[pgbouncer]
listen_port = {listen_port}
listen_addr = 127.0.0.1
logfile = {log_file}
pidfile = {pid_file}
auth_type = md5
auth_file = {auth_file}
admin_users = {pgb_user}
            '''.format(
                host = self.db_hostname,
                user = self.db_username,
                password = self.db_password,
                port = self.db_port,
                database = self.db_database,
                log_file = self.log_file,
                pid_file = self.pid_file,
                auth_file =self.auth_file,
                listen_port = self.port,
                pgb_user =  self.user,
                pgb_database = self.database
            ))

    def create_auth_file(self):
        def quoted(s):
            return '"' +  s + '"'
        with open(self.auth_file,'w') as f:
            f.write(quoted(self.user) + " " + quoted(self.password))

    db_uri_template = "postgresql://{user}:{password}@localhost:{port}/{db}"

    def set_pgbouncer_files_owner(self, user):
        if os.getuid() == 0:
            pw_rec = pwd.getpwnam(user)
            uid = pw_rec.pw_uid
            gid = pw_rec.pw_gid
            for f in [self.conf_dir, self.auth_file, self.ini_file, self.log_file]:
                os.chown(f, uid, gid)

    def get_pgbouncer_url(self):
        return self.db_uri_template.format(
            user = self.user,
            password = self.password,
            port = self.port,
            db = self.database
        )

    def get_pgbouncer_ctrl_url(self):
        return self.db_uri_template.format(
            user = self.user,
            password = self.password,
            port = self.port,
            db = 'pgbouncer'
        )

    def start(self):
        print(Fore.YELLOW, "Starting PGBouncer proxy on port " + str(self.port), Style.RESET_ALL)
        os.makedirs(self.conf_dir, exist_ok=True)
        self.create_auth_file()
        self.create_ini_file()
        Path(self.log_file).touch()
        pgbouncer_args = ['pgbouncer', '-d', self.ini_file]
        if os.getuid() == 0:
            user = 'nobody'
            self.set_pgbouncer_files_owner(user)
            self.process = popen_user(user, pgbouncer_args)
        else:
            self.process = subprocess.Popen(
                pgbouncer_args
            )
        tests_info_db.add_reserved_process_port(self.port, self.process.pid, 'pgbouncer')

    def stop(self):
        try:
            print(Fore.YELLOW, "Stopping PGBouncer running on port " + str(self.port), Style.RESET_ALL)
            conn = psycopg2.connect(self.get_pgbouncer_ctrl_url())
            conn.autocommit = True
            conn.cursor().execute("SHUTDOWN")
        except psycopg2.OperationalError:
            pass
        self.process.wait()
        tests_info_db.remove_process_ports(self.process.pid)
        #pg_run_sql_without_transaction(self.get_pgbouncer_ctrl_url(), "SHUTDOWN;")

    def restart(self, delay_after_stop=0):
        self.stop()
        if delay_after_stop > 0:
            time.sleep(delay_after_stop)
        self.start()

