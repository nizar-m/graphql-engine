import pytest
from skip_test_modules import skip_module
from conftest import per_class_db_context

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)

@pytest.mark.parametrize("transport", ['http','websocket'])
@per_class_db_context
class TestAllowlistQueries:

    dir = 'queries/graphql_query/allowlist'

    def test_query_user(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_user.yaml', transport)

    def test_query_user_by_pk(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_user_by_pk.yaml', transport)

    def test_query_user_with_typename(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_user_with_typename.yaml', transport)

    def test_query_non_allowlist(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_non_allowlist.yaml', transport)

    def test_query_as_admin(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_as_admin.yaml', transport)

    def test_update_query(self, hge_ctx, transport):
        # test only for http
        transport = 'http'
        hge_ctx.check_query_f(self.dir + '/update_query.yaml', transport)
