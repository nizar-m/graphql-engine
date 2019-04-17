import pytest
import yaml
from validate import check_query_f
from conftest import graphql_engine_test_context

# @pytest.mark.parametrize("transport", ['http', 'websocket'])
# graphql parser can't seem to parse {where: null}, disabling
# websocket till then
@pytest.mark.parametrize("transport", ['http'])
@graphql_engine_test_context
class TestGraphQLValidation:

    dir =  "queries/graphql_validation"

    def test_null_value(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + "/null_value_err.yaml", transport)

    def test_null_variable_value(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + "/null_variable_value_err.yaml", transport)
