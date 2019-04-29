import pytest
from validate import check_query_f
from super_classes import DefaultTestSelectQueries

if not pytest.config.getoption("--test-dev-mode"):
    pytest.skip("--test-dev-mode flag is missing, skipping tests", allow_module_level=True)

@pytest.mark.parametrize("transport", ['http','websocket']) 
@pytest.mark.usefixtures("inconsistent_hge_catalog")
class TestDevModeGraphqlQuery(DefaultTestSelectQueries):
    
    @pytest.fixture(scope='class')
    def inconsistent_hge_catalog(self, transact, hge_ctx):
        hge_ctx.sql("alter table jsonb_table alter column jsonb_col type text")
        yield
        hge_ctx.sql("alter table jsonb_table alter column jsonb_col type jsonb using jsonb_col::jsonb")

    def test_user_query_with_catalog_inconsistency(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + '/dev_mode_user_query_with_inconsistent_catalog_err.yaml', transport)

    @classmethod
    def dir(cls):
        return 'queries/graphql_query/permissions'
