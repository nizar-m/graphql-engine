#!/usr/bin/env python3

import subprocess
import os
import yaml
import requests
import validate
import jinja2
from jinja2 import TemplateSyntaxError
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from test_components import auth_webhook

class HGECtxError(Exception):
    pass

class HGETestSvcsConf:
    def __init__(self, remote_gql_root_url=None, evts_webhook_root_url=None):
        self.remote_gql_root_url = remote_gql_root_url
        self.evts_webhook_root_url = evts_webhook_root_url

    def get_conf(self):
        conf = {}
        for attr in ['remote_gql_root_url', 'evts_webhook_root_url']:
            if getattr(self, attr, None):
                conf[attr] = getattr(self, attr)
        return conf


class HGECtx:

    def verify_webhook(self):
        if not self.hge_webhook:
            return
        if self.webhook_insecure:
            return
        else:
            print ("CA: ", os.environ['REQUESTS_CA_BUNDLE'])
            auth_webhook.verify_auth_webhook(self.hge_webhook)

    def __init__(self, hge_url, pg_url, hge_key, hge_webhook, webhook_insecure,
                 hge_jwt_key_file, hge_jwt_conf, metadata_disabled, ws_read_cookie, hge_replica_url, hge_log_file, hge_version):

        self.http = requests.Session()
        self.hge_key = hge_key
        self.hge_url = hge_url
        self.pg_url = pg_url
        self.hge_webhook = hge_webhook
        if hge_jwt_key_file is None:
            self.hge_jwt_key = None
        else:
            with open(hge_jwt_key_file) as f:
                self.hge_jwt_key = f.read()
        self.hge_jwt_conf = hge_jwt_conf
        self.webhook_insecure = webhook_insecure
        self.metadata_disabled = metadata_disabled
        self.may_skip_test_teardown = False

        self.engine = create_engine(self.pg_url)
        self.meta = MetaData()

        self.ws_read_cookie = ws_read_cookie

        self.hge_replica_url = hge_replica_url
        self.hge_log_file = hge_log_file

        self.verify_webhook()

        self.version = hge_version
        if not self.version:
            result = subprocess.run(['../../scripts/get-version.sh'], shell=False, stdout=subprocess.PIPE, check=True)
            self.version = result.stdout.decode('utf-8').strip()

        self.services_conf = HGETestSvcsConf()

        if not self.metadata_disabled:
          try:
              st_code, resp = self.admin_v1q_f('queries/clear_db.yaml')
          except requests.exceptions.RequestException as e:
              self.teardown()
              raise HGECtxError(repr(e))
          assert st_code == 200, resp

    def reflect_tables(self):
        self.meta.reflect(bind=self.engine)

    def anyq(self, u, q, h):
        resp = self.http.post(
            self.hge_url + u,
            json=q,
            headers=h
        )
        return resp.status_code, resp.json()

    def sql(self, q):
        with self.engine.connect() as conn:
            return conn.execute(q)

    def admin_v1q(self, q, headers = {}):
        return self.admin_v1q_url(q, self.hge_url, headers)

    def admin_v1q_url(self, q, hge_url, headers={}):
        h = headers.copy()
        if self.hge_key is not None:
            h['X-Hasura-Admin-Secret'] = self.hge_key
        resp = self.http.post(
            hge_url + "/v1/query",
            json=q,
            headers=h
        )
        return resp.status_code, resp.json()

    def admin_v1q_f(self, filename):
        conf = self.render_if_jinja(filename)
        return self.admin_v1q(conf)

    def check_query_f(self, conf_file, transport='http', add_auth=True):
        conf = self.render_if_jinja(conf_file)
        return validate.check_query_yaml_conf(self, conf, transport, add_auth)

    def render_if_jinja(self, conf_file):
        print("Test configuration file: " + conf_file)
        r = None
        try:
            with open(conf_file) as f:
                tmplt = jinja2.Template(f.read())
            data = self.services_conf.get_conf()
            print("Render data: ", data)
            r = tmplt.render(data)
        except TemplateSyntaxError:
            with open(conf_file) as f:
                r = f.read()
        return yaml.safe_load(r)

    def teardown(self):
        self.http.close()
        self.engine.dispose()
