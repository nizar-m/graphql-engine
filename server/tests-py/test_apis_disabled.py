#!/usrbin/env python3

import pytest
from validate import check_query
from skip_test_modules import skip_module

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)

def check_post_404(hge_ctx,url):
   return check_query(hge_ctx, {
     'url': url,
     'status': 404,
     'query': {}
   })

@pytest.fixture(scope='class')
def skip_tests_based_on_flags(request):
   '''
   If Metadata APIs are enabled/disabled, skip the metadata disabled/enabled tests
   Similarly for GraphQL APIs
   '''
   cls = request.cls
   class_name = cls.__name__
   flag = getattr(cls, 'skip_if_flag_set', None)
   if flag and request.config.getoption(flag):
      pytest.skip('{}: Flag {} is set. Skipping'.format(class_name, flag))

   flag = getattr(cls, 'skip_if_flag_not_set', None)
   if flag and not request.config.getoption(flag):
      pytest.skip('{}: Flag {} is NOT set. Skipping'.format(class_name, flag))

pytestmark = pytest.mark.usefixtures('skip_tests_based_on_flags')

class TestMetadataDisabled:

    skip_if_flag_not_set = '--test-metadata-disabled'

    def test_metadata_v1_query_disabled(self, hge_ctx):
        check_post_404(hge_ctx,'/v1/query')

    def test_metadata_v1_template_disabled(self, hge_ctx):
        check_post_404(hge_ctx,'/v1/template/foo')

    def test_metadata_api_1_disabled(self, hge_ctx):
        check_post_404(hge_ctx,'/api/1/table/foo/select')


class TestGraphQLDisabled:

    skip_if_flag_not_set = '--test-graphql-disabled'

    def test_graphql_endpoint_disabled(self, hge_ctx):
        check_post_404(hge_ctx, '/v1/graphql')

    def test_graphql_explain_disabled(self, hge_ctx):
        check_post_404(hge_ctx, '/v1/graphql/explain')


class TestGraphQLEnabled:

    skip_if_flag_set = '--test-graphql-disabled'

    def test_graphql_introspection(self, hge_ctx):
        hge_ctx.check_query_f("queries/graphql_introspection/introspection_only_kind_of_queryType.yaml")


class TestMetadataEnabled:

    skip_if_flag_set = '--test-metadata-disabled'

    def test_reload_metadata(self, hge_ctx):
        hge_ctx.check_query_f("queries/v1/metadata/reload_metadata.yaml")

    def test_run_sql(self, hge_ctx):
        hge_ctx.check_query_f("queries/v1/run_sql/sql_set_timezone.yaml")


