import pytest
from conftest import per_class_db_context

# graphql parser can't seem to parse {where: null}, disabling
# websocket till then
@pytest.mark.parametrize("transport", ['http'])
@per_class_db_context
class TestGraphQLValidation:

    dir =  "queries/graphql_validation"

    def test_null_value(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/null_value_err.yaml", transport)

    def test_null_variable_value(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/null_variable_value_err.yaml", transport)
