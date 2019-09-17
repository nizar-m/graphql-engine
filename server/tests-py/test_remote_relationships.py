#!/usr/bin/env python3

import pytest
import yaml

from validate import check_query_f
from test_schema_stitching import add_remote, delete_remote
import time


@pytest.fixture(scope='class')
def remote_get_url(remote_gql_server):
    def remote_url(path):
        return remote_gql_server.root_url + path
    return remote_url

def run_sql(hge_ctx, sql):
    query = {
        'type': 'run_sql',
        'args': {
            'sql' : sql
        }
    }
    st_code, resp = hge_ctx.v1q(query)
    assert st_code == 200, resp
    return resp

def delete_remote_rel(hge_ctx, table_schema, table_name, name):
    query = {
        "type" : "delete_remote_relationship",
        "args" : {
            "table" : {
                "schema" : table_schema,
                "name" : table_name
            },
            "name" : name
        }
    }
    st_code, resp = hge_ctx.v1q(query)
    assert st_code == 200, resp
    return resp

def delete_all_remote_relationships(hge_ctx):
    return [ delete_remote_rel(hge_ctx, ts, tn, name)
             for (ts, tn, name)
             in get_all_remote_relationships(hge_ctx)
    ]

def get_all_remote_relationships(hge_ctx):
    query = {
        "type" : "select",
        "args" : {
            "table" : {
                "schema" : "hdb_catalog",
                "name" : "hdb_remote_relationship"
            },
            "columns" : [
                'table_schema',
                'table_name',
                'name'
            ]
        }
    }
    st_code, resp = hge_ctx.v1q(query)
    assert st_code == 200, resp
    return [(rel['table_schema'],rel['table_name'],rel['name']) for rel in resp]

@pytest.fixture(scope='class')
def validate_v1q_f(hge_ctx, request):
    def cvq(f, exp_code = 200):
        st_code, resp = hge_ctx.v1q_f(request.cls.dir() + f)
        assert st_code == exp_code, {
            'response': resp,
            'file' : f
        }
        return (st_code, resp)
    return cvq

class TestCreateRemoteRelationship:

    @classmethod
    def dir(cls):
        return "queries/remote_schemas/remote_relationships/"

    remote_schemas = {
        'user' : '/user-graphql',
        'prefixer-proxy': '/graphql-prefixer-proxy'
    }

    @pytest.fixture(autouse=True, scope='function')
    def cleanup_remote_relationships(self, hge_ctx, db_state):
        yield
        delete_all_remote_relationships(hge_ctx)

    @pytest.fixture(autouse=True, scope='class')
    def db_state(self, hge_ctx, remote_get_url, validate_v1q_f):
        validate_v1q_f('setup.yaml')
        for (schema, path) in self.remote_schemas.items():
            add_remote(hge_ctx, schema, remote_get_url(path))
        yield
        for schema in self.remote_schemas:
            delete_remote(hge_ctx, schema)
        validate_v1q_f('teardown.yaml')

    def test_create_basic(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_basic.yaml')

    def test_create_nested(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_nested_args.yaml')

    def test_create_with_array(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_array.yaml')

    def test_create_nested_fields(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_nested_fields.yaml')

    def test_create_multiple_hasura_fields(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_multiple_fields.yaml')

    @pytest.mark.xfail(reason="Refer https://github.com/tirumaraiselvan/graphql-engine/issues/53")
    def test_create_arg_with_arr_struture(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_arg_with_arr_structure.yaml')

    @pytest.mark.xfail(reason="Refer https://github.com/tirumaraiselvan/graphql-engine/issues/15")
    def test_create_enum_arg(self, validate_v1q_f):
        validate_v1q_f('setup_remote_rel_enum_arg.yaml')

    # st_code, resp = hge_ctx.v1q_f(self.dir() + 'setup_remote_rel_with_interface.yaml')
    # assert st_code == 200, resp

    def test_create_invalid_hasura_field(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_hasura_field.yaml', 400)

    def test_create_invalid_remote_rel_arg_literal(self, validate_v1q_f):
        """Test with input argument literal not having the required type"""
        validate_v1q_f('setup_invalid_remote_rel_literal.yaml', 400)

    def test_create_invalid_remote_rel_variable(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_variable.yaml', 400)

    def test_create_invalid_remote_args(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_remote_args.yaml', 400)

    def test_create_invalid_remote_schema(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_remote_schema.yaml', 400)

    def test_create_invalid_remote_field(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_remote_field.yaml', 400)

    def test_create_invalid_remote_rel_type(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_type.yaml', 400)

    def test_create_invalid_remote_rel_nested_args(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_nested_args.yaml', 400)

    def test_create_invalid_remote_rel_array(self, validate_v1q_f):
        validate_v1q_f('setup_invalid_remote_rel_array.yaml', 400)

    def test_generation(self, validate_v1q_f, hge_ctx):
        validate_v1q_f('setup_remote_rel_basic.yaml')
        validate_v1q_f('setup_remote_rel_nested_args.yaml')
        check_query_f(hge_ctx, self.dir() + 'select_remote_fields.yaml')

class TestDeleteRemoteRelationship:
    @classmethod
    def dir(cls):
        return "queries/remote_schemas/remote_relationships/"

    @pytest.fixture(autouse=True, scope='class')
    def db_schema_and_data(self, validate_v1q_f):
        validate_v1q_f('setup.yaml')
        yield
        validate_v1q_f('teardown.yaml')

    @pytest.fixture(autouse=True, scope='function')
    def remotes_and_relationsips(self, hge_ctx, remote_get_url, validate_v1q_f):
        add_remote(hge_ctx, "user", remote_get_url("/user-graphql"))
        validate_v1q_f('setup_remote_rel_basic.yaml')
        yield
        delete_all_remote_relationships(hge_ctx)
        delete_remote(hge_ctx, "user")

    def test_delete(self, validate_v1q_f):
        validate_v1q_f('delete_remote_rel.yaml')

    def test_delete_dependencies(self, validate_v1q_f):
        validate_v1q_f('remove_remote_schema.yaml', 400)
        validate_v1q_f('delete_remote_rel.yaml')

class TestUpdateRemoteRelationship:
    @classmethod
    def dir(cls):
        return "queries/remote_schemas/remote_relationships/"

    remote_schemas = {
        'user' : '/user-graphql',
        'message' : '/messages-graphql',
        'prefixer-proxy': '/graphql-prefixer-proxy'
    }

    @pytest.fixture(autouse=True, scope='class')
    def db_schema_and_data(self, validate_v1q_f):
        validate_v1q_f('setup.yaml')
        yield
        validate_v1q_f('teardown.yaml')

    @pytest.fixture(autouse=True, scope='function')
    def remotes_and_relationsips(self, hge_ctx, remote_get_url, validate_v1q_f):
        for (schema, path) in self.remote_schemas.items():
            add_remote(hge_ctx, schema, remote_get_url(path))
        validate_v1q_f('setup_remote_rel_basic.yaml')
        yield
        delete_all_remote_relationships(hge_ctx)
        for schema in self.remote_schemas:
            delete_remote(hge_ctx, schema)

    def test_update(self, hge_ctx, validate_v1q_f):
        check_query_f(hge_ctx, self.dir() + 'basic_relationship.yaml')
        validate_v1q_f('update_remote_rel_basic.yaml')
        check_query_f(hge_ctx, self.dir() + 'update_basic_query.yaml')

@pytest.mark.parametrize("transport", ['http'])
class TestExecution:

    @classmethod
    def dir(cls):
        return "queries/remote_schemas/remote_relationships/"

    remote_schemas = {
        'messages' : '/messages-graphql',
        'user' : '/user-graphql',
        'error' : '/error-graphql',
        'prefixer-proxy': '/graphql-prefixer-proxy'
    }

    @pytest.fixture(autouse=True, scope='class')
    def db_schema_and_data(self, validate_v1q_f):
        validate_v1q_f('setup.yaml')
        yield
        validate_v1q_f('teardown.yaml')

    @pytest.fixture(autouse=True, scope='class')
    def remotes_and_rel(self, validate_v1q_f, hge_ctx, remote_get_url, db_schema_and_data):
        for (schema, path) in self.remote_schemas.items():
            add_remote(hge_ctx, schema, remote_get_url(path))
        for cf in [
                'setup_remote_rel_basic.yaml',
                'setup_remote_rel_nested_args.yaml',
                'setup_remote_rel_array.yaml',
                'setup_remote_rel_multiple_fields.yaml',
                'setup_remote_rel_nested_fields.yaml',
                'setup_remote_rel_scalar.yaml',
                'setup_remote_rel_with_errors.yaml' ]:
            validate_v1q_f(cf)
        yield
        delete_all_remote_relationships(hge_ctx)
        for schema in self.remote_schemas:
            delete_remote(hge_ctx, schema)

    def test_basic_mixed(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_mixed.yaml', transport)

    def test_basic_relationship(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_relationship.yaml', transport)

    def test_basic_relationship_on_object(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_object_rel.yaml', transport)

    def test_basic_relationship_on_object_with_arr_rel(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_arr_rel.yaml', transport)

    def test_basic_array(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_array.yaml', transport)

    def test_basic_array_without_join_key(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_array_without_join_key.yaml', transport)

    def test_multiple_fields(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_multiple_fields.yaml', transport)

    def test_nested_fields(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_nested_fields.yaml', transport)

    @pytest.mark.xfail(reason = "Order not preserved")
    def test_arguments(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_arguments.yaml', transport)

    def test_arguments_err_same_arg_as_definition(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_arguments_err_same_arg_as_definition.yaml', transport)

    def test_with_mixed_variables(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'mixed_variables.yaml', transport)

    def test_with_remote_rel_variables(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'remote_rel_variables.yaml', transport)

    def test_with_fragments_mixed_top_level_fields(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'mixed_fragments.yaml', transport)

    def test_with_fragments_on_remote_relationship(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'remote_rel_fragments.yaml', transport)

    # TODO: Support interface in remote relationships
    # def test_with_interface(self, hge_ctx, transport):
    #     check_query_f(hge_ctx, self.dir() + 'mixed_interface.yaml', transport)
    #     check_query_f(hge_ctx, self.dir() + 'remote_rel_interface.yaml', transport)

    @pytest.fixture(scope='function')
    def add_new_profiles(self, hge_ctx):
        """
        This fixture is specific to the test below for errors with object remote field.
        We add some user profiles into the profiles table, which will not be present in the remote graphql server.
        The remote relationship should fail to get the users for these newly added profiles.
        """
        with open(self.dir() + 'fixture_add_profiles.yaml') as f:
            conf = yaml.safe_load(f)
        run_sql(hge_ctx, conf['add'])
        yield
        run_sql(hge_ctx, conf['remove'])

    @pytest.mark.usefixtures("add_new_profiles")
    def test_with_remote_errors_obj_field(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_errors_obj.yaml', transport)

    def test_with_remote_errors_arr_field(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_errors_arr.yaml', transport)

    def test_with_scalar_relationship(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_scalar_rel.yaml', transport)

    def test_multiple_relationships_with_distinct_remotes(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_two_remote_rels_on_distinct_remotes.yaml', transport)


@pytest.mark.parametrize("transport", ['http'])
class TestDeepExecution:

    remote_schemas = {
        'user' : '/user-graphql',
        'prefixer-proxy': '/graphql-prefixer-proxy'
    }

    @classmethod
    def dir(cls):
        return "queries/remote_schemas/remote_relationships/"

    @pytest.fixture(autouse=True, scope='class')
    def db_schema_and_data(self, validate_v1q_f):
        validate_v1q_f('setup.yaml')
        yield
        validate_v1q_f('teardown.yaml')

    @pytest.fixture(autouse=True, scope='class')
    def remotes_and_rel(self, validate_v1q_f, hge_ctx, remote_get_url, db_schema_and_data):
        for (schema, path) in self.remote_schemas.items():
            add_remote(hge_ctx, schema, remote_get_url(path))
        for cf in [
                'setup_remote_rel_basic.yaml',
                'setup_remote_rel_nested_args.yaml']:
            validate_v1q_f(cf)
        yield
        delete_all_remote_relationships(hge_ctx)
        for schema in self.remote_schemas:
            delete_remote(hge_ctx, schema)

    def test_with_hasura_deep_nested_object(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_hasura_deep_nesting_obj.yaml', transport)

    def test_with_remote_deep_nested_object(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_remote_deep_nesting_obj.yaml', transport)

    def test_with_deep_array(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_deep_nesting_arr.yaml', transport)

    def test_with_deep_nesting_on_hasura_and_remote(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_deep_nesting_on_hasura_and_remote.yaml', transport)

    def test_with_hasura_and_two_remotes_at_different_nesting_levels(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_hasura_and_two_remotes_at_different_nesting_levels.yaml', transport)

    def test_with_complex_path_array_1(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_deep_nesting_complex_path_arr.yaml', transport)

    def test_with_complex_path_array_2(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'query_with_deep_nesting_complex_path_arr2.yaml', transport)


@pytest.mark.parametrize("transport", ['http'])
class TestExecutionWithPermissions:

    remote_schemas = {
        'user' : '/user-graphql',
        'prefixer-proxy': '/graphql-prefixer-proxy'
    }

    @classmethod
    def dir(cls):
        return "queries/remote_schemas/remote_relationships/"

    @pytest.fixture(autouse=True, scope='class')
    def db_schema_and_data(self, validate_v1q_f):
        validate_v1q_f('setup.yaml')
        yield
        validate_v1q_f('teardown.yaml')

    @pytest.fixture(autouse=True, scope='class')
    def remotes_and_rel(self, validate_v1q_f, hge_ctx, remote_get_url, db_schema_and_data):
        for (schema, path) in self.remote_schemas.items():
            add_remote(hge_ctx, schema, remote_get_url(path))
        for cf in [
                'setup_remote_rel_basic.yaml',
                'setup_remote_rel_nested_args.yaml']:
            validate_v1q_f(cf)
        yield
        delete_all_remote_relationships(hge_ctx)
        for schema in self.remote_schemas:
            delete_remote(hge_ctx, schema)

    def test_basic_relationship(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_relationship_with_permissions.yaml', transport)

    def test_basic_relationship_err(self, hge_ctx, transport):
        check_query_f(hge_ctx, self.dir() + 'basic_relationship_with_permissions_err.yaml', transport)
