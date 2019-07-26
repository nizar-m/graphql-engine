import pytest
import yaml
import time
import jsondiff

from validate import validate_json, json_equals
from skip_test_modules import skip_module

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)


get_metadata_q =  {
    'args': {},
    'type' :  'metadata_export'
}


def wait_for_metadata_sync(hge_ctx, wait_time=20):
    assert wait_time > 0, "Timeout waiting for metadata sync of replicas"
    if has_metadata_synced(hge_ctx):
        return True
    time.sleep(1)
    return wait_for_metadata_sync(hge_ctx, wait_time - 1)


def has_metadata_synced(hge_ctx):
    metadatas = [get_metadata(hge_ctx, hge_url) for hge_url in [hge_ctx.hge_url, hge_ctx.hge_replica_url]]
    return json_equals(metadatas[0], metadatas[1])


def get_metadata(hge_ctx, hge_url):
    return hge_ctx.admin_v1q_url(get_metadata_q, hge_url)


class TestHorizantalScaleBasic():

    servers = {}

    dir = 'queries/horizontal_scale/basic'


    @pytest.fixture(autouse=True, scope='class')
    def transact(self, hge_ctx):
        self.servers['first_replica'] = hge_ctx.hge_url
        self.servers['second_replica'] = hge_ctx.hge_replica_url
        yield
        # teardown
        st_code, resp = hge_ctx.admin_v1q_f(self.dir + '/teardown.yaml')
        assert st_code == 200, resp


    def test_horizontal_scale_basic(self, hge_ctx):
        with open(self.dir + "/steps.yaml") as c:
            conf = yaml.safe_load(c)

        assert isinstance(conf, list) == True, 'Not an list'
        for _, step in enumerate(conf):
            # execute operation
            response = hge_ctx.http.post(
                self.servers[step['operation']['server']] + "/v1/query",
                json=step['operation']['query']
            )
            st_code = response.status_code
            resp = response.json()
            assert st_code == 200, resp

            # wait for 20 sec
            time.sleep(1)
            wait_for_metadata_sync(hge_ctx, 15)
            # validate data
            response = hge_ctx.http.post(
                self.servers[step['validate']['server']] + "/v1/graphql",
                json=step['validate']['query']
            )
            st_code = response.status_code
            resp = response.json()
            assert st_code == 200, resp

            if 'response' in step['validate']:
                validate_json(resp, step['validate']['response'])
