import pytest
from skip_test_modules import skip_module

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)

def v1qCompat(hge_ctx, q, headers = {}):
        h = {'X-Hasura-Access-Key': hge_ctx.hge_key}
        resp = hge_ctx.http.post(
            hge_ctx.hge_url + "/v1/query",
            json=q,
            headers=h
        )
        return resp.status_code, resp.json()

class TestGraphQLCompatAccessKey():

    export_metadata = {
       "type" : "export_metadata",
       "args" : {}
    }

    def test_compact_access_key_export_metadata(self, hge_ctx):
        code, resp = v1qCompat(hge_ctx, self.export_metadata)
        assert code == 200, resp
