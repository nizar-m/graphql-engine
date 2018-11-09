#!/usr/bin/env python3

import pytest
import queue
import yaml
import time
from validate import check_event, check_query_f, assert_no_event
from super_classes import DefaultTestQueries

if pytest.config.getoption("--hge-jwt-key-file"):
    pytest.skip("Skipping event based tests when JWT auth is present", allow_module_level=True)

if pytest.config.getoption("--hge-webhook"):
    pytest.skip("Skipping event based tests when webhook is present", allow_module_level=True)

def select_last_event_fromdb(hge_ctx):
    q = {
        "type": "select",
        "args": {
            "table": {"schema": "hdb_catalog", "name": "event_log"},
            "columns": ["*"],
            "order_by": ["-created_at"],
            "limit": 1
        }
    }
    st_code, resp = hge_ctx.v1q(q)
    return st_code, resp


def insert(hge_ctx, table, rows, returning=[]):
    q = {
        "type": "insert",
        "args": {
            "table": table,
            "objects": rows,
            "returning": returning
        }
    }
    st_code, resp = hge_ctx.v1q(q)
    return st_code, resp


def update(hge_ctx, table, where_exp, set_exp):
    q = {
        "type": "update",
        "args": {
            "table": table,
            "where": where_exp,
            "$set": set_exp
        }
    }
    st_code, resp = hge_ctx.v1q(q)
    return st_code, resp


def delete(hge_ctx, table, where_exp):
    q = {
        "type": "delete",
        "args": {
            "table": table,
            "where": where_exp
        }
    }
    st_code, resp = hge_ctx.v1q(q)
    return st_code, resp


class TestCreateEvtPermissions(DefaultTestQueries):

    def test_create_trigger_as_user_err(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + "/create_trigger_as_user_err.yaml")

    def test_delete_trigger_as_user_err(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + "/delete_trigger_as_user_err.yaml")

    @classmethod
    def dir(cls):
        return "queries/event_triggers/permissions"

class TestCreateEvtQuery(object):

    @classmethod
    def dir(cls):
        return "queries/event_triggers/basic"

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f(self.dir() + '/setup.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f(self.dir() + '/teardown.yaml')
        assert st_code == 200, resp

    def test_t1_all_operations_all_cols(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + '/create_event_trigger_t1_all.yaml')

        table = {"schema": "hge_tests", "name": "test_t1"}

        init_row = {"c1": 1, "c2": "hello", "c3" : { "foo" : "bar" }  }
        exp_ev_data = {
            "old": None,
            "new": init_row
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_all", table, "INSERT", exp_ev_data, headers, "/")

        where_exp = {'c1': 1}
        set_exp = {'c2': 'world'}
        mod_row = dict(init_row)
        mod_row['c2'] = 'world'
        exp_ev_data = {
            "old": init_row,
            "new": mod_row
        }
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_all", table, "UPDATE", exp_ev_data, headers, "/")

        exp_ev_data = {
            "old": mod_row,
            "new": None
        }
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_all", table, "DELETE", exp_ev_data, headers, "/")

        check_query_f(hge_ctx, self.dir() + '/delete_event_trigger_t1_all.yaml')

    def test_t1_table_quoted_cols(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + '/create_event_trigger_quoted_table_cols.yaml')

        table = {"schema": "hge_tests", "name": "testQuoted"}
        init_row = {"tC1": 1, "tC2": "hello"}
        exp_ev_data = {
            "old": None,
            "new": { "tC2": "hello" }
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        check_event(hge_ctx, "tQuoted", table, "INSERT", exp_ev_data, headers, "/")

        where_exp = {"tC2": "hello"}
        set_exp = {"tC1": 2}
        exp_ev_data = {
            "old": {"tC1": 1},
            "new": {"tC1": 2}
        }
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        assert resp['affected_rows'] == 1, resp
        check_event(hge_ctx, "tQuoted", table, "UPDATE", exp_ev_data, headers, "/")

        exp_ev_data = {
            "old": {"tC2": "hello"},
            "new": None
        }
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "tQuoted", table, "DELETE", exp_ev_data, headers, "/")

        check_query_f(hge_ctx, self.dir() + '/delete_event_trigger_quoted_table.yaml')

    def test_create_trigger_no_operation_defined_err(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + '/create_event_trigger_no_oper_defined_err.yaml')

    def test_create_insert_trigger_no_columns_defined_err(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + '/create_insert_trigger_no_cols_defined_err.yaml')

    def test_create_trigger_empty_payload_defined_err(self, hge_ctx):
        check_query_f(hge_ctx, self.dir() + '/create_insert_trigger_empty_payload_defined_err.yaml')


class TestRetryConf(object):

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/retry_conf/setup.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/retry_conf/teardown.yaml')
        assert st_code == 200, resp

    def test_basic(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        init_row = {"c1": 1, "c2": "hello"}
        exp_ev_data = {
            "old": None,
            "new": init_row
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        time.sleep(8)
        tries = hge_ctx.get_error_queue_size()
        assert tries == 4, tries


class TestEvtHeaders(object):

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/headers/setup.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/headers/teardown.yaml')
        assert st_code == 200, resp

    def test_basic(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        init_row = {"c1": 1, "c2": "hello"}
        exp_ev_data = {
            "old": None,
            "new": init_row
        }
        headers = {"X-Header-From-Value": "MyValue", "X-Header-From-Env": "MyEnvValue"}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_all", table, "INSERT", exp_ev_data, headers, "/")


class TestUpdateEvtQuery(object):

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/update_query/create-setup.yaml')
        assert st_code == 200, resp
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/update_query/update-setup.yaml')
        assert st_code == 200, '{}'.format(resp)
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/update_query/teardown.yaml')
        assert st_code == 200, resp

    def test_only_update_events(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        init_rows = [
            {"c1": 1, "c2": "hello"},
            {"c1": 2, "c2": "foo"}
        ]
        headers = {}
        st_code, resp = insert(hge_ctx, table, init_rows)
        assert st_code == 200, resp

        where_exp = {"c1": 1}
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        assert_no_event(hge_ctx, "INSERT and DELETE")

        c2_new =  'bar'
        where_exp = {'c1': 2}
        set_exp = {'c2': c2_new}
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        new_row_2 = dict(init_rows[1])
        new_row_2['c2'] = c2_new
        exp_ev_data = {
            "old": init_rows[1],
            "new": new_row_2
        }
        check_event(hge_ctx, "t1_update", table, "UPDATE", exp_ev_data, headers, "/new")


class TestDeleteEvtQuery(object):

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/delete_query/setup.yaml')
        assert st_code == 200, '{}'.format(resp)
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/delete_query/teardown.yaml')
        assert st_code == 200, resp

    def test_delete_events_only(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        #Insert: no event expected
        init_row = {"c1": 1, "c2": "hello"}
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp

        #Update: no event expected
        where_exp = {"c1": 1}
        set_exp = {"c2": "world"}
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        assert_no_event(hge_ctx, "INSERT and UPDATE")

        #Delete: event expected
        exp_ev_data = {
            "old": {"c1": 1, "c2": "world"},
            "new": None
        }
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        assert resp['affected_rows'] == 1, resp
        check_event(hge_ctx, "t1_delete", table, "DELETE", exp_ev_data, headers, "/")


class TestEvtSelCols:

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/selected_cols/setup.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/selected_cols/teardown.yaml')
        assert st_code == 200, resp

    def test_selected_cols(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        init_row = {"c1": 1, "c2": "hello"}
        exp_ev_data = {
            "old": None,
            "new": {"c1": 1, "c2": "hello"}
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_cols", table, "INSERT", exp_ev_data, headers, "/")

        where_exp = {"c1": 1}
        set_exp = {"c2": "world"}
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        assert_no_event(hge_ctx, "UPDATE on column c2")

        where_exp = {"c1": 1}
        set_exp = {"c1": 2}
        exp_ev_data = {
            "old": {"c1": 1, "c2": "world"},
            "new": {"c1": 2, "c2": "world"}
        }
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_cols", table, "UPDATE", exp_ev_data, headers, "/")

        where_exp = {"c1": 2}
        exp_ev_data = {
            "old": {"c1": 2, "c2": "world"},
            "new": None
        }
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_cols", table, "DELETE", exp_ev_data, headers, "/")

    def test_selected_cols_dep(self, hge_ctx):
        st_code, resp = hge_ctx.v1q({
            "type": "run_sql",
            "args": {
                "sql": "alter table hge_tests.test_t1 drop column c1"
            }
        })
        assert st_code == 400, resp
        assert resp['code'] == "dependency-error", resp

        st_code, resp = hge_ctx.v1q({
            "type": "run_sql",
            "args": {
                "sql": "alter table hge_tests.test_t1 drop column c2"
            }
        })
        assert st_code == 200, resp


class TestEvtInsertOnly:

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/insert_only/setup.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/insert_only/teardown.yaml')
        assert st_code == 200, resp

    def test_insert_only(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        #Insert: Expecting an event
        init_row = {"c1": 1, "c2": "hello"}
        exp_ev_data = {
            "old": None,
            "new": init_row
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_insert", table, "INSERT", exp_ev_data, headers, "/")

        #Update: no event expected
        where_exp = {"c1": 1}
        set_exp = {"c2": "world"}
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        assert resp['affected_rows'] == 1, resp

        #Delete: no event expected
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        assert resp['affected_rows'] == 1, resp
        assert_no_event(hge_ctx, "UPDATE and DELETE")

        #Second insert: Expecting an event
        init_row_2 = {"c1": 2, "c2": "world"}
        exp_ev_data_2 = {
            "old": None,
            "new": init_row_2
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row_2])
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_insert", table, "INSERT", exp_ev_data_2, headers, "/")


class TestEvtSelPayload:

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/selected_payload/setup.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f('queries/event_triggers/selected_payload/teardown.yaml')
        assert st_code == 200, resp

    def test_selected_payload(self, hge_ctx):
        table = {"schema": "hge_tests", "name": "test_t1"}

        init_row = {"c1": 1, "c2": "hello"}
        exp_ev_data = {
            "old": None,
            "new": {"c1": 1, "c2": "hello"}
        }
        headers = {}
        st_code, resp = insert(hge_ctx, table, [init_row])
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_payload", table, "INSERT", exp_ev_data, headers, "/")

        where_exp = {"c1": 1}
        set_exp = {"c2": "world"}
        exp_ev_data = {
            "old": {"c1": 1},
            "new": {"c1": 1}
        }
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_payload", table, "UPDATE", exp_ev_data, headers, "/")

        where_exp = {"c1": 1}
        set_exp = {"c1": 2}
        exp_ev_data = {
            "old": {"c1": 1},
            "new": {"c1": 2}
        }
        st_code, resp = update(hge_ctx, table, where_exp, set_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_payload", table, "UPDATE", exp_ev_data, headers, "/")

        where_exp = {"c1": 2}
        exp_ev_data = {
            "old": {"c2": "world"},
            "new": None
        }
        st_code, resp = delete(hge_ctx, table, where_exp)
        assert st_code == 200, resp
        check_event(hge_ctx, "t1_payload", table, "DELETE", exp_ev_data, headers, "/")

    def test_selected_payload_dep(self, hge_ctx):
        st_code, resp = hge_ctx.v1q({
            "type": "run_sql",
            "args": {
                "sql": "alter table hge_tests.test_t1 drop column c1"
            }
        })
        assert st_code == 400, resp
        assert resp['code'] == "dependency-error", resp

        st_code, resp = hge_ctx.v1q({
            "type": "run_sql",
            "args": {
                "sql": "alter table hge_tests.test_t1 drop column c2"
            }
        })
        assert st_code == 400, resp
        assert resp['code'] == "dependency-error", resp
