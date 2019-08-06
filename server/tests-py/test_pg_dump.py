import yaml
import os
from conftest import per_class_db_context
from packaging.version import Version


def get_resp_type(ver):
    if Version(ver) < Version('10.0.0'):
        return 'response_9' 
    else:
        return 'response_10_11'


@per_class_db_context
class TestPGDump:

    dir = 'pgdump'

    def test_pg_dump_for_public_schema(self, hge_ctx):
        res = hge_ctx.sql('SHOW server_version')
        pg_version = res.fetchone()['server_version']
        query_file = self.dir + '/pg_dump_public.yaml'
        with open(query_file, 'r') as stream:
            q = yaml.safe_load(stream)
            headers = {}
            if hge_ctx.hge_key is not None:
                headers['x-hasura-admin-secret'] = hge_ctx.hge_key
            resp = hge_ctx.http.post(hge_ctx.hge_url + q['url'], json=q['query'], headers=headers)
            body = resp.text
            assert resp.status_code == q['status'], body
            assert body == q[get_resp_type(pg_version)], body

