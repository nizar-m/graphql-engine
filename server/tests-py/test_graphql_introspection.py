import yaml
from validate import check_query
from conftest import select_queries_context
from test_schema_stitching import get_types, get_type_by_name, get_fld_by_name

@select_queries_context
class TestGraphqlIntrospection:

    dir = "queries/graphql_introspection"

    def test_introspection(self, hge_ctx):
        with open(self.dir + "/introspection.yaml") as c:
            conf = yaml.safe_load(c)
        resp = check_query(hge_ctx, conf)
        types = get_types(resp)
        article = get_type_by_name(types, 'article')
        assert article is not None
        author_manual_rel = get_fld_by_name(article, 'author_obj_rel_manual')
        assert_fld_type_kind(author_manual_rel, 'OBJECT')
        author_fk_rel = get_fld_by_name(article, 'author_obj_rel_fk')
        assert_fld_type_kind(author_fk_rel, 'NON_NULL')

    def test_introspection_user(self, hge_ctx):
        hge_ctx.check_query_f(self.dir + "/introspection_user_role.yaml")

def get_fld_type_kind(fld):
    return fld['type']['kind']

def assert_fld_type_kind(fld, kind):
    assert fld is not None
    assert get_fld_type_kind(fld) == kind
