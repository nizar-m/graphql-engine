import pytest
import time
import webserver
from fixture_modules.remote_graphql_server import gql_server_handlers
from fixture_modules.hge_websocket_client import hge_ws_client
from fixture_modules.events_webhook import EvtsWebhookServer
from fixture_modules.context import HGECtx, HGECtxError
import threading
import random
import sys
import os
import socket
import pathlib
import tempfile
import shutil
from colorama import Fore, Style
from skip_test_modules import set_skip_test_module_rules

def pytest_addoption(parser):
    parser.addoption(
        "--hge-urls",
        metavar="HGE_URLS",
        help="List of urls for graphql-engine",
        required=False,
        nargs='+'
    )

    parser.addoption(
        "--pg-urls", metavar="PG_URLS",
        help="List of urls for connecting to Postgres directly",
        required=False,
        nargs='+'
    )
    parser.addoption(
        "--hge-key", metavar="HGE_KEY", help="admin secret key for graphql-engine", required=False
    )
    parser.addoption(
        "--hge-webhook", metavar="HGE_WEBHOOK", help="url for graphql-engine's access control webhook", required=False
    )
    parser.addoption(
        "--test-webhook-insecure", action="store_true",
        help="Run Test cases for insecure https webhook"
    )
    parser.addoption(
        "--hge-jwt-key-file", metavar="HGE_JWT_KEY_FILE", help="File containting the private key used to encode jwt tokens using RS512 algorithm", required=False
    )
    parser.addoption(
        "--hge-jwt-conf", metavar="HGE_JWT_CONF", help="The JWT conf", required=False
    )

    parser.addoption(
        "--test-cors", action="store_true",
        required=False,
        help="Run testcases for CORS configuration"
    )

    parser.addoption(
        "--test-ws-init-cookie",
        metavar="read|noread",
        required=False,
        help="Run testcases for testing cookie sending over websockets"
    )

    parser.addoption(
        "--test-metadata-disabled", action="store_true",
        help="Run Test cases with metadata queries being disabled"
    )

    parser.addoption(
        "--test-graphql-disabled", action="store_true",
        help="Run Test cases with GraphQL queries being disabled"
    )

    parser.addoption(
        "--hge-replica-urls",
        metavar="HGE_REPLICA_URLS",
        required=False,
        help="List of urls of graphql-engine replicas",
        nargs='+'
    )

    parser.addoption(
        "--evts-webhook-ports",
        metavar="EVENTS_WEBHOOK_PORTS",
        type=int,
        required=False,
        help="List of ports to be used by event webhooks",
        nargs='+'
    )

    parser.addoption(
        "--remote-gql-ports",
        metavar="EVENTS_WEBHOOK_PORTS",
        type=int,
        required=False,
        help="List of ports to be used by event webhooks",
        nargs='+'
    )

    parser.addoption(
        "--test-allowlist-queries", action="store_true",
        help="Run Test cases with allowlist queries enabled"
    )

    parser.addoption(
        "--test-logging",
        action="store_true",
        default=False,
        required=False,
        help="Run testcases for logging"
    )

    parser.addoption(
        "--hge-log-files",
        metavar="HGE_LOG_FILES",
        help="List of graphql-engine log files",
        required=False,
        nargs='+'
    )

    parser.addoption(
        "--hge-version",
        metavar="HGE_VERSION",
        help="Version of Hasura graphql engine. If not specified, the version will be obtained by running get-version.sh",
        required=False,
    )



#By default,
#1) Set default parallelism to one
#2) Set test grouping to by filename (--dist=loadfile)
def pytest_cmdline_preparse(config, args):
    worker = os.environ.get('PYTEST_XDIST_WORKER')
    if 'xdist' in sys.modules and not worker and not 'no:xdist' in args:  # pytest-xdist plugin
        num = 1
        args[:] = ['-n' + str(num), '--dist=loadfile', '--max-worker-restart=0'] + args

def ensure_uniqueness(l, name):
    if len(l) != len(unique_list(l)):
        exit(name + ' are not unique: ' + str(l))

def ensure_enough_parallel_configs(l, name, threads):
    if threads > len(l):
        exit('Not enough unique ' + name + ' specified, Required ' + str(threads) + ', got ' + str(len(l)))

def pytest_configure(config):
    if is_master(config):
        for o in ['--hge-urls', '--pg-urls']:
            if not config.getoption(o):
                exit(o + ' should be specified')

        config.hge_urls = config.getoption('--hge-urls').copy()
        config.pg_urls = config.getoption('--pg-urls').copy()

        #This directory would store the error messages thrown by xdist threads
        #Hack for https://github.com/pytest-dev/pytest-xdist/issues/86
        config.tmpdir = tempfile.mkdtemp()

        xdist_threads = config.getoption('-n', default=None) or 1

        for (opt, start_port) in ('--evts-webhook-ports', 5592), ('--remote-gql-ports', 6000):
            attr = opt[2:].replace('-', '_')
            if config.getoption(opt):
                setattr(config, attr, config.getoption(opt).copy())
            else:
                setattr(config, attr, get_unused_ports(start_port, xdist_threads))

        uniq_list = [
            (config.pg_urls, 'Postgres URLs'),
            (config.hge_urls, 'HGE urls'),
            (config.evts_webhook_ports, 'Events webhook ports'),
            (config.remote_gql_ports, 'Remote GraphQL engine ports')
        ]

        for (opt, detail) in [('--hge-replica-urls', 'HGE replica urls'),('--hge-log-files', 'HGE log files')]:
            attr = opt[2:].replace('-', '_')
            if config.getoption(opt):
                setattr(config, attr, config.getoption(opt).copy())
                uniq_list.append((getattr(config, attr), detail))


        for (l,n) in uniq_list:
            ensure_uniqueness(l,n)
            if xdist_threads > 1:
                ensure_enough_parallel_configs(l, n, xdist_threads)

    set_skip_test_module_rules(config)
    #pytest modifies stderr to capture the test outputs, which also prevents from showing errors during pytest.exit(err)
    #Saving the original stderr to config so that errors can be shown
    random.seed()


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node):
    for attr in [ 'hge_url', 'pg_url', 'evts_webhook_port', 'remote_gql_port' ]:
        node.slaveinput[attr] = getattr(node.config, attr + 's').pop()

    for attr in ['hge_replica_url','hge_log_file']:
        if hasattr(node.config, attr + 's'):
            node.slaveinput[attr] = getattr(node.config, attr + 's').pop()

    node.slaveinput['tmpdir'] = node.config.tmpdir


def pytest_unconfigure(config):
    if is_master(config):
        try:
            for filename in os.listdir(config.tmpdir):
                with open(config.tmpdir + "/" + filename) as f:
                    sys.stderr.write(f.read())
        finally:
            shutil.rmtree(config.tmpdir)


#We cannot override a class level parameterization with a function level one in pytest (while using pytest.mark.parametrize).
#So we are using the 'transport' marker to implement overrides.
#With the following function, the closest 'transport' marker is used to parameterize tests.
def pytest_generate_tests(metafunc):
    transport = metafunc.definition.get_closest_marker('transport')
    if transport:
        metafunc.parametrize('transport', [pytest.param(o, marks=getattr(pytest.mark,o)) for o in transport.args])

@pytest.fixture(scope='module')
def hge_ctx(request):
    """
    This fixture sets the main context for tests
    """
    config = request.config
    print("create hge_ctx")
    try:
        hge_ctx = HGECtx(
            hge_url = get_config(config, 'hge-url'),
            pg_url  = get_config(config, 'pg-url'),
            hge_key = config.getoption('--hge-key'),
            hge_webhook = config.getoption('--hge-webhook'),
            webhook_insecure = config.getoption('--test-webhook-insecure'),
            hge_jwt_key_file = config.getoption('--hge-jwt-key-file'),
            hge_jwt_conf = config.getoption('--hge-jwt-conf'),
            ws_read_cookie = config.getoption('--test-ws-init-cookie'),
            metadata_disabled = config.getoption('--test-metadata-disabled'),
            hge_replica_url = get_config_or_none(config, 'hge-replica-url'),
            hge_log_file = get_config_or_none(config, 'hge-log-file'),
            hge_version = config.getoption('--hge-version')
        )
    except HGECtxError as e:
        if not is_master(config):
            # Exit messages are not shown properly if xdist plugin is present (https://github.com/pytest-dev/pytest-xdist/issues/86)
            # Hack around this by saving the errors into a file, and print them out during pytest_unconfigure
            log_file = config.slaveinput['tmpdir'] + "/" + config.slaveinput['workerid'] + "_err.log"
            with open(log_file, 'w') as f:
                f.write(Style.BRIGHT + '[' + config.slaveinput['workerid'] + '] ' + Fore.RED + 'HGECtxError: ' + str(e) + Style.RESET_ALL + '\n')
        pytest.exit(str(e))

    yield hge_ctx  # provide the fixture value
    print("teardown hge_ctx")
    hge_ctx.teardown()
    time.sleep(1)

@pytest.fixture(scope='module')
def remote_gql_server(request, hge_ctx):
    """
    This fixture runs the remote GraphQL server needed for tests with Remote GraphQL servers
    """
    port = get_config(request.config, 'remote-gql-port')

    remote_gql_httpd = webserver.WebServer(
        ('127.0.0.1', port),
        gql_server_handlers
    )
    gql_server = start_webserver(remote_gql_httpd)
    wait_for_port_open(port)
    hge_ctx.services_conf.remote_gql_root_url = 'http://127.0.0.1:' + str(port)
    yield remote_gql_httpd
    stop_webserver(gql_server)
    hge_ctx.services_conf.remote_gql_root_url = None

@pytest.fixture(scope='class')
def evts_webhook(request, hge_ctx):
    """
    This fixture returns the events webhook server
    """
    port = get_config(request.config, 'evts-webhook-port')
    webhook_httpd = EvtsWebhookServer(server_address=('127.0.0.1',port))
    webserver = start_webserver(webhook_httpd)
    hge_ctx.services_conf.evts_webhook_root_url = 'http://127.0.0.1:' + str(port)
    yield webhook_httpd
    stop_webserver(webserver)
    hge_ctx.services_conf.evts_webhook_root_url = None

def start_webserver(httpd):
    webserver = threading.Thread(target=httpd.serve_forever)
    webserver.httpd = httpd
    webserver.start()
    return webserver

def stop_webserver(webserver):
    webserver.httpd.shutdown()
    webserver.httpd.server_close()
    webserver.join()

@pytest.fixture(scope='class')
def remote_gql_url(remote_gql_server):
    '''
    Get the url of the remote GraphQL server endpoint with the given path.
    '''
    return get_url_func(remote_gql_server)

@pytest.fixture(scope='class')
def evts_webhook_url(evts_webhook):
    '''
    Get the url of the events webhook with the given path.
    '''
    return get_url_func(evts_webhook)

@pytest.fixture(scope='function')
def ws_client(request, hge_ctx):
    """
    This fixture provides a websocket client that supports Apollo protocol
    """
    endpoint = request.cls.websocket_endpoint
    assert endpoint.endswith('graphql'), "Invalid websocket endpoint" + endpoint
    with hge_ws_client(hge_ctx, endpoint) as client:
        yield client

per_class_db_context = pytest.mark.usefixtures('per_class_db_state')

select_queries_context = pytest.mark.usefixtures('per_class_db_state')

mutations_context = pytest.mark.usefixtures('db_schema_for_mutations', 'db_data_for_mutations')

ddl_queries_context = pytest.mark.usefixtures('per_method_db_state')

metadata_ops_context = ddl_queries_context

any_query_context = ddl_queries_context

evts_db_state_context = pytest.mark.usefixtures('per_method_db_state', 'evts_webhook')

@pytest.fixture(scope='class')
def per_class_db_state(request, hge_ctx):
    """"
    This fixture sets up the database state for select queries.
    A class level scope would work, as select queries does not change database state
    """
    setup = getattr(request.cls, 'setup_files', request.cls.dir + '/setup.yaml')
    teardown = getattr(request.cls, 'teardown_files', request.cls.dir + '/teardown.yaml')
    yield from setup_and_teardown(hge_ctx, setup, teardown)

@pytest.fixture(scope='class')
def db_schema_for_mutations(request, hge_ctx):
    """"
    This fixture sets up the database schema for mutations
    This can have a class level scope, since mutations does not change schema
    """
    setup = getattr(request.cls, 'schema_setup_files', request.cls.dir + '/schema_setup.yaml')
    teardown = getattr(request.cls, 'schema_teardown_files', request.cls.dir + '/schema_teardown.yaml')
    yield from setup_and_teardown(hge_ctx, setup, teardown)

@pytest.fixture(scope='function')
def db_data_for_mutations(request, hge_ctx, db_schema_for_mutations):
    """"
    This fixture sets up the data for mutations
    Requires a function level scope, since mutations may change data
    """
    setup = getattr(request.cls, 'data_setup_files', request.cls.dir + '/data_setup.yaml')
    teardown = getattr(request.cls, 'data_teardown_files', request.cls.dir + '/data_teardown.yaml')
    yield from setup_and_teardown(hge_ctx, setup, teardown, False)

@pytest.fixture(scope='function')
def per_method_db_state(request, db_state_info, hge_ctx):
    """"
    This fixture sets up the database state for metadata operations
    Requires a function level scope, since metadata operations may change both the schema and data
    """
    setup_if_reqd(request, db_state_info, hge_ctx)
    yield
    teardown_if_reqd(request, db_state_info, hge_ctx)

@pytest.fixture(scope='class')
def db_state_info(request, hge_ctx):
    """
    This fixture helps in tracking the overall db setup status per class.
    It also sets up database state before the first test
    and tears down database state after the last test in the class.
    """
    db_state_info = { "setup_done" : False }
    setup_if_reqd(request, db_state_info, hge_ctx)
    yield db_state_info
    hge_ctx.may_skip_test_teardown = False
    teardown_if_reqd(request, db_state_info, hge_ctx)

def get_url_func(server):
    def get_url(path):
        (host, port) = server.server_address
        return 'http://127.0.0.1:{}{}'.format(port, path)
    return get_url

def setup_if_reqd(request, db_state_info, hge_ctx):
    setup = getattr(request.cls, 'setup_files', request.cls.dir + '/setup.yaml')
    def v1q_f(f):
        st_code, resp = hge_ctx.admin_v1q_f(f)
        assert st_code == 200, resp
    if not db_state_info['setup_done']:
        run_elem_or_list(v1q_f, setup)
        db_state_info['setup_done'] = True

def teardown_if_reqd(request, db_state_info, hge_ctx):
    teardown = getattr(request.cls, 'teardown_files', request.cls.dir + '/teardown.yaml')
    def v1q_f(f):
        st_code, resp = hge_ctx.admin_v1q_f(f)
        assert st_code == 200, resp
    if db_state_info['setup_done'] and not hge_ctx.may_skip_test_teardown:
        run_elem_or_list(v1q_f, teardown)
        db_state_info['setup_done'] = False

def setup_and_teardown(hge_ctx, setup, teardown, check_file_exists=True):
    def assert_file_exists(f):
        assert pathlib.Path(f).exists(), 'Could not find file ' + f
    if check_file_exists:
        for o in [setup, teardown]:
            run_elem_or_list(assert_file_exists, o)
    def v1q_f(f):
        if pathlib.Path(f).exists():
            st_code, resp = hge_ctx.admin_v1q_f(f)
            assert st_code == 200, resp
    run_elem_or_list(v1q_f, setup)
    yield
    run_elem_or_list(v1q_f, teardown)

def run_elem_or_list(f, x):
    if isinstance(x, str):
        return f(x)
    elif isinstance(x, list):
        return [f(e) for e in x]

def get_config_or_none(config, attr):
    attr = attr.replace('-','_')
    if is_master(config):
        return getattr(config, attr + 's',[None])[0]
    else:
        return config.slaveinput.get(attr)

def get_config(config, attr):
    attr = attr.replace('-','_')
    if is_master(config):
        return getattr(config, attr + 's')[0]
    else:
        return config.slaveinput[attr]

def is_master(config):
    """True if the code running the given pytest.config object is running in a xdist master
    node or not running xdist at all.
    """
    return not hasattr(config, 'slaveinput')

def unique_list(x):
    return list(set(x))

def exit(e):
    pytest.exit(Fore.RED + Style.BRIGHT + e + Style.RESET_ALL)

def wait_for_port_open(port, max_wait=10):
    start_time = time.time()
    while time.time() - start_time < max_wait:
        if is_port_open(port):
            return True
        else:
            time.sleep(0.2)
    else:
        return False

def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('127.0.0.1', port))
        return res == 0

def get_unused_port(start):
    if is_port_open(start):
        return get_unused_port(start + 1)
    else:
        return start

def get_unused_ports(start, count):
    ports = []
    for i in range(0, count):
        port = get_unused_port(start)
        ports.append(port)
        start = port + 1
    return ports
