import pytest
from conftest import select_queries_context
from skip_test_modules import skip_module

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)

@select_queries_context
class TestHTTPSWebhookInsecure:

    dir = 'webhook/insecure'

    def test_user_select_unpublished_articles_err(self, hge_ctx):
        hge_ctx.check_query_f(self.dir + '/user_select_query_unpublished_articles_fail.yaml')

    def test_user_only_other_users_published_articles_err(self, hge_ctx):
        hge_ctx.check_query_f(self.dir + '/user_query_other_users_published_articles_fail.yaml')

