import pytest
from conftest import select_queries_context
from skip_test_modules import skip_module

skip_reason = skip_module(__file__)
if skip_reason:
    pytest.skip(skip_reason, allow_module_level=True)

transport = pytest.mark.tansport

pytestmark = [select_queries_context, transport('http', 'websoket')]

class TestGraphQLQueryBasic:

    dir = 'queries/graphql_query/basic'

    def test_select_query_author(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_author.yaml', transport)

    def test_select_various_postgres_types(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_test_types.yaml', transport)

    def test_select_query_author_quoted_col(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_author_col_quoted.yaml', transport)

    def test_select_query_author_pk(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_author_by_pkey.yaml', transport)

    def test_select_query_where(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_author_where.yaml', transport)

    def test_nested_select_query_article_author(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/nested_select_query_article_author.yaml', transport)

    def test_nested_select_query_deep(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/nested_select_query_deep.yaml', transport)

    def test_nested_select_query_where(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/nested_select_where_query_author_article.yaml', transport)

    def test_nested_select_query_where_on_relationship(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/nested_select_query_article_author_where_on_relationship.yaml', transport)

    def test_select_query_user(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/select_query_user.yaml", transport)

    def test_select_query_non_tracked_table(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/select_query_non_tracked_table_err.yaml", transport)

    def test_select_query_col_not_present_err(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/select_query_author_col_not_present_err.yaml", transport)

    def test_select_query_user_col_change(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/select_query_user_col_change.yaml")

    @transort('http')
    def test_nested_select_with_foreign_key_alter(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/nested_select_with_foreign_key_alter.yaml", transport)


class TestGraphQLQueryAgg:

    dir = 'queries/graphql_query/aggregations'

    def test_article_agg_count_sum_avg_max_min_with_aliases(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/article_agg_count_sum_avg_max_min_with_aliases.yaml', transport)

    def test_article_agg_where(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/article_agg_where.yaml', transport)

    def test_author_agg_with_articles(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/author_agg_with_articles.yaml', transport)

    def test_author_agg_with_articles_where(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/author_agg_with_articles_where.yaml', transport)

    def test_article_deeply_nested_aggregate(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/article_deeply_nested_aggregate.yaml', transport)


class TestGraphQLQueryAggPerm:

    dir = 'queries/graphql_query/agg_perm'

    def test_author_agg_articles(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/author_agg_articles.yaml', transport)

    def test_article_agg_fail(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/article_agg_fail.yaml', transport)

    def test_author_articles_agg_fail(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/author_articles_agg_fail.yaml', transport)


class TestGraphQLQueryLimits:

    dir = 'queries/graphql_query/limits'

    def test_limit_1(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_limit_1.yaml', transport)

    def test_limit_2(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_limit_2.yaml', transport)

    @transport('http')
    def test_limit_null(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_limit_null.yaml', transport)

    def test_err_str_limit_error(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_string_limit_error.yaml', transport)

    def test_err_neg_limit_error(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_neg_limit_error.yaml', transport)


class TestGraphQLQueryOffsets:

    dir = 'queries/graphql_query/offset'

    def test_offset_1_limit_2(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_offset_1_limit_2.yaml', transport)

    def test_offset_2_limit_1(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_offset_2_limit_1.yaml', transport)

    def test_int_as_string_offset(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_string_offset.yaml', transport)

    @transport('http')
    def test_err_neg_offset_error(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_article_neg_offset_error.yaml', transport)


class TestGraphQLQueryBoolExpBasic:

    dir = 'queries/graphql_query/boolexp/basic'

    def test_author_article_where_not_equal(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_neq.yaml', transport)

    def test_author_article_operator_ne_not_found_err(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_operator_ne_not_found_err.yaml', transport)

    def test_author_article_where_greater_than(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_gt.yaml', transport)

    def test_author_article_where_greater_than_or_equal(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_gte.yaml', transport)

    def test_author_article_where_less_than(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_lt.yaml', transport)

    def test_author_article_where_less_than_or_equal(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_lte.yaml', transport)

    def test_author_article_where_in(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_in.yaml', transport)

    def test_author_article_where_in_empty_array(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_in_empty_array.yaml', transport)

    def test_author_article_where_nin_empty_array(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_nin_empty_array.yaml', transport)

    def test_author_article_where_nin(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_nin.yaml', transport)

    def test_uuid_test_in_uuid_col(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_uuid_test_in_uuid_col.yaml', transport)

    def test_order_delivered_at_is_null(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_order_delivered_at_is_null.yaml', transport)

    def test_order_delivered_at_is_not_null(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_query_order_delivered_at_is_not_null.yaml', transport)

    def test_author_article_where_not_less_than(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_where_not_lt.yaml', transport)

    def test_article_author_is_published_and_registered(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_article_author_is_published_and_registered.yaml', transport)

    def test_article_author_not_published_nor_registered(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_article_author_not_published_or_not_registered.yaml', transport)

    def test_article_author_unexpected_operator_in_where_err(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_unexpected_operator_in_where_err.yaml', transport)

    def test_self_referential_relationships(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/self_referential_relationships.yaml', transport)


class TestGraphqlQueryPermissions:

    dir = 'queries/graphql_query/permissions'

    def test_user_select_unpublished_articles(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_select_query_unpublished_articles.yaml', transport)

    def test_user_only_other_users_published_articles(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_can_query_other_users_published_articles.yaml', transport)

    def test_anonymous_only_published_articles(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/anonymous_can_only_get_published_articles.yaml', transport)

    def test_user_cannot_access_remarks_col(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_cannot_access_remarks_col.yaml', transport)

    def test_user_can_query_geometry_values_filter(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_can_query_geometry_values_filter.yaml', transport)

    def test_user_can_query_geometry_values_filter_session_vars(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_can_query_geometry_values_filter_session_vars.yaml', transport)

    def test_user_can_query_jsonb_values_filter(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_can_query_jsonb_values_filter.yaml', transport)

    def test_user_can_query_jsonb_values_filter_session_vars(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/user_can_query_jsonb_values_filter_session_vars.yaml', transport)

    def test_artist_select_query_Track_fail(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/artist_select_query_Track_fail.yaml', transport)

    def test_artist_select_query_Track(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/artist_select_query_Track.yaml', transport)

    def test_artist_search_tracks(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/artist_search_tracks.yaml', transport)

    def test_artist_search_tracks_aggregate(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/artist_search_tracks_aggregate.yaml', transport)

    def test_staff_passed_students(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/staff_passed_students.yaml', transport)

    def test_user_query_auction(self, hge_ctx, transport):
        hge_ctx.check_query_f(hge_ctx, self.dir() + '/user_query_auction.yaml', transport)


class TestGraphQLQueryBoolExpSearch:

    dir = 'queries/graphql_query/boolexp/search'

    def test_city_where_like(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_city_where_like.yaml', transport)

    def test_city_where_not_like(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_city_where_nlike.yaml', transport)

    def test_city_where_ilike(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_city_where_ilike.yaml', transport)

    def test_city_where_not_ilike(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_city_where_nilike.yaml', transport)

    def test_city_where_similar(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_city_where_similar.yaml', transport)

    def test_city_where_not_similar(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_city_where_not_similar.yaml', transport)


class TestGraphQLQueryBoolExpJsonB:

    dir = 'queries/graphql_query/boolexp/jsonb'

    def test_jsonb_contains_article_latest(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_article_author_jsonb_contains_latest.yaml', transport)

    def test_jsonb_contains_article_beststeller(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_author_article_jsonb_contains_bestseller.yaml', transport)

    def test_jsonb_contained_in_latest(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_article_author_jsonb_contained_in_latest.yaml', transport)

    def test_jsonb_contained_in_bestseller_latest(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_article_author_jsonb_contained_in_bestseller_latest.yaml', transport)

    def test_jsonb_has_key_sim_type(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_product_jsonb_has_key_sim_type.yaml', transport)

    def test_jsonb_has_keys_any_os_operating_system(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_product_jsonb_has_keys_any_os_operating_system.yaml', transport)

    def test_jsonb_has_keys_all_touchscreen_ram(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/select_product_jsonb_has_keys_all_ram_touchscreen.yaml', transport)


class TestGraphQLQueryBoolExpPostGIS:

    dir = 'queries/graphql_query/boolexp/postgis'

    def test_query_using_point(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_using_point.yaml', transport)

    def test_query_using_line(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_using_line.yaml', transport)

    def test_query_using_polygon(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_using_polygon.yaml', transport)

    def test_query_geography_spatial_ops(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/query_geography_spatial_ops.yaml', transport)


class TestGraphQLQueryOrderBy:

    dir = 'queries/graphql_query/order_by'

    def test_articles_order_by_without_id(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/articles_order_by_without_id.yaml', transport)

    def test_articles_order_by_rel_author_id(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/articles_order_by_rel_author_id.yaml', transport)

    def test_articles_order_by_rel_author_rel_contact_phone(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/articles_order_by_rel_author_rel_contact_phone.yaml', transport)

    def test_album_order_by_tracks_count(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/album_order_by_tracks_count.yaml', transport)

    def test_album_order_by_tracks_duration_avg(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/album_order_by_tracks_duration_avg.yaml', transport)

    def test_album_order_by_tracks_max_name(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/album_order_by_tracks_max_name.yaml', transport)

    def test_album_order_by_tracks_bytes_stddev(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/album_order_by_tracks_bytes_stddev.yaml', transport)

    def test_employee_distinct_department_order_by_salary_desc(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/employee_distinct_department_order_by_salary_desc.yaml', transport)

    def test_employee_distinct_department_order_by_salary_asc(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/employee_distinct_department_order_by_salary_asc.yaml', transport)

    def test_employee_distinct_fail(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/employee_distinct_fail.yaml', transport)


class TestGraphQLQueryFunctions:

    dir = 'queries/graphql_query/functions'

    def test_search_posts(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/query_search_posts.yaml")

    def test_search_posts_aggregate(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + "/query_search_posts_aggregate.yaml")

    @transport('http')
    def test_alter_function_error(self, hge_ctx, transport):
        hge_ctx.check_query_f(self.dir + '/alter_function_error.yaml', transport)

    def test_overloading_function_error(self, hge_ctx):
        hge_ctx.check_query_f(self.dir + '/overloading_function_error.yaml')

    def test_query_get_test_uuid(self, hge_ctx):
        hge_ctx.check_query_f(self.dir + '/query_get_test_uuid.yaml')

    def test_query_my_add(self, hge_ctx):
        hge_ctx.check_query_f(hge_ctx, self.dir() + '/query_my_add.yaml')
