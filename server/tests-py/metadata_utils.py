from functools import partial
import pytest


def mk_v1q_setup_fn(f_q, hge_ctx):
    def v1q_setup_impl(*args, **kwargs):
        (setup, teardown) = f_q(*args, **kwargs)
        code, resp = hge_ctx.v1q(setup)
        assert code == 200, resp
        yield
        code, resp = hge_ctx.v1q(teardown)
        assert code == 200, resp
    return v1q_setup_impl


def mk_v1q_fn(f_q, hge_ctx):
    def v1q_impl(*args, **kwargs):
        code, resp = hge_ctx.v1q(f_q(*args, **kwargs))
        assert code == 200, resp
        return resp
    return v1q_impl


def run_sql_q(sql, cascade=False):
    return {
        'type' : 'run_sql',
        'args': {
            'sql': sql,
            'cascade': cascade
        }
    }


@pytest.fixture(scope='function')
def run_sql(hge_ctx):
    return mk_v1q_fn(run_sql_q, hge_ctx)

def delete_table_q(table):
    return run_sql_q(
        "delete table {} cascade".format(table_to_sql(table)),
        True
    )

@pytest.fixture(scope='function')
def delete_table(hge_ctx):
    return mk_v1q_fn(delete_table_q, hge_ctx)


def track_table_q(table):
    return {
        'type': 'track_table',
        'args': {
            'table':  table
        }
    }


@pytest.fixture(scope='function')
def track_table(hge_ctx):
    return mk_v1q_fn(track_table_q, hge_ctx)


def untrack_table_q(table):
    return {
        'type': 'untrack_table',
        'args': {
            'table': table
        }
    }


@pytest.fixture(scope='function')
def untrack_table(hge_ctx):
    return mk_v1q_fn(untrack_table_q, hge_ctx)


def create_rel_q(_type, name, table, using):
    return {
        'type': 'create_' + _type + '_relationship',
        'args': {
            'table': table,
            'name': name,
            'using': using
        }
    }


def create_obj_fk_rel_q(name, table, fk_col):
    using = { 'foreign_key_constraint_on': fk_col }
    return create_rel_q('object', name, table, using)


@pytest.fixture(scope='function')
def create_obj_fk_rel(hge_ctx):
    return mk_v1q_fn(create_obj_fk_rel_q, hge_ctx)

def create_obj_manual_rel_q(name, table, remote_table, column_mapping):
    using = {
        'manual_configuration': {
            'remote_table': remote_table,
            'column_mapping': column_mapping
        }
    }
    return create_rel_q('object', name, table, using)


@pytest.fixture(scope='function')
def create_obj_manual_rel(hge_ctx):
    return mk_v1q_fn(create_obj_manual_rel_q, hge_ctx)


def create_arr_fk_rel_q(name, table, remote_table, remote_fk_col):
    using = {
        'foreign_key_constraint_on': {
            'table': remote_table,
            'column': remote_fk_col
        }
    }
    return create_rel_q('array', name, table, using)


@pytest.fixture(scope='function')
def create_arr_fk_rel(hge_ctx):
    return mk_v1q_fn(create_arr_fk_rel_q, hge_ctx)


def create_arr_manual_rel_q(name, table, remote_table, column_mapping):
    using = {
        'manual_configuration': {
            'remote_table': remote_table,
            'column_mapping': column_mapping
        }
    }
    return create_rel_q('array', name, table, using)

@pytest.fixture(scope='function')
def create_arr_manual_rel(hge_ctx):
    return mk_v1q_fn(create_arr_manual_rel_q, hge_ctx)

def drop_rel_q(name, table, cascade=False):
    return {
        'type': 'drop_relationship',
        'args': {
            'table': table,
            'name': name,
            'cascade': cascade
        }
    }

@pytest.fixture(scope='function')
def drop_rel(hge_ctx):
    return mk_v1q_fn(create_arr_manual_rel_q, hge_ctx)

def create_perm_q(_type, role, table, perm):
    return {
        'type': 'create_' + _type + '_permission',
        'args': {
            'table': table,
            'role': role,
            'permission': perm
        }
    }

def create_ins_perm_q(role, table, check, columns=None, presets=None):
    perm = {
        'set' : presets,
        'check': check,
        'columns': columns
    }
    return create_perm_q('insert', role, table, perm)


@pytest.fixture(scope='function')
def create_ins_perm(hge_ctx):
    return mk_v1q_fn(create_ins_perm_q, hge_ctx)


def create_sel_perm_q(role, table, _filter, columns, limit=None, allow_aggr=False):
    perm = {
        'columns': columns,
        'filter': _filter,
        'limit': limit,
        'allow_aggregations': allow_aggr
    }
    return create_perm_q('select', role, table, perm)


@pytest.fixture(scope='function')
def create_sel_perm(hge_ctx):
    return mk_v1q_fn(create_sel_perm_q, hge_ctx)


def create_upd_perm_q(role, table, _filter, columns, presets=None):
    perm = {
        'columns': columns,
        'filter': _filter,
        'set': presets
    }
    return create_perm_q('update', role, table, perm)


@pytest.fixture(scope='function')
def create_upd_perm(hge_ctx):
    return mk_v1q_fn(create_upd_perm_q, hge_ctx)


def create_del_perm_q(role, table, _filter):
    return create_perm_q('delete', role, table, { 'filter': _filter })


@pytest.fixture(scope='function')
def create_del_perm(hge_ctx):
    return mk_v1q_fn(create_del_perm_q, hge_ctx)


def drop_perm_q(_type, role, table):
    return {
        'type': 'drop_' + _type + '_permission' ,
        'args': {
            'table': table,
            'role': role
        }
    }


drop_ins_perm_q = partial(drop_perm_q, 'insert')


@pytest.fixture(scope='function')
def drop_ins_perm(hge_ctx):
    return mk_v1q_fn(drop_ins_perm_q, hge_ctx)


drop_sel_perm_q = partial(drop_perm_q, 'select')


@pytest.fixture(scope='function')
def drop_sel_perm(hge_ctx):
    return mk_v1q_fn(drop_sel_perm_q, hge_ctx)


drop_upd_perm_q = partial(drop_perm_q, 'update')


@pytest.fixture(scope='function')
def drop_upd_perm(hge_ctx):
    return mk_v1q_fn(drop_upd_perm_q, hge_ctx)

drop_del_perm_q = partial(drop_perm_q, 'delete')


@pytest.fixture(scope='function')
def drop_del_perm(hge_ctx):
    return mk_v1q_fn(drop_del_perm_q, hge_ctx)


def add_remote_q(name, url, headers=None, client_hdrs=False, timeout=None):
    return {
        "type": "add_remote_schema",
        "args": {
            "name": name,
            "comment": "testing " + name,
            "definition": {
                "url": url,
                "headers": headers,
                "forward_client_headers": client_hdrs,
                "timeout_seconds": timeout
            }
        }
    }


@pytest.fixture(scope='function')
def add_remote(hge_ctx):
    return mk_v1q_fn(add_remote_q, hge_ctx)

def reload_remote_q(name):
    return {
        "type" : "reload_remote_schema",
        "args" : {
            "name" : name
        }
    }

def delete_remote_q(name):
    return {
        "type" : "remove_remote_schema",
        "args" : {
            "name": name
        }
    }

def setup_remote_q(name, url, headers=None, client_hdrs=False, timeout=None):
    return (
        add_remote_q(name, url, headers, client_hdrs, timeout),
        delete_remote_q(name)
    )


@pytest.fixture(scope='function')
def setup_remote(hge_ctx):
    '''
    Returns a generator which provides the setup and teardown of a remote
    Can be used in a fixture which requires a remote to be setup.
    Example:

    @pytest.fixture()
    def setup_my_remote(self, setup_remote):
         yield from setup_remote('name_of_remote', url, ..other_args)

    This is equivalent to
    @pytest.fixture()
    def transact(self, add_remote, delete_remote)
         setup_remote('name_of_remote', url, ..other_args)
         yield
         delete_remote('name_of_remote')
    '''
    return mk_v1q_setup_fn(setup_remote_q, hge_ctx)


@pytest.fixture(scope='function')
def delete_remote(hge_ctx):
    return mk_v1q_fn(delete_remote_q, hge_ctx)


def table_to_sql(table):
    if isinstance(table, str):
        return in_quotes(table)
    elif isinstance(table, dict):
        return in_quotes(table['schema']) + "." + in_quotes(table['name'])


def in_quotes(x):
  return '"' + x + '"'
