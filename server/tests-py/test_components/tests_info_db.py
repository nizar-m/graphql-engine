import sqlite3
import psutil
import os
import functools

sqlite_db = None

def output_dir():
    return os.environ.get('HASURA_TEST_OUTPUT_FOLDER','graphql-engine-test-output')

def setup_tests_info_db():
    global sqlite_db
    if sqlite_db:
        return True #Tests DB has already been setup

    sqlite_db_env = os.environ.get('HASURA_TEST_INFO_DB', output_dir() + '/tests_info.db')
    if not sqlite_db_env:
        return False #Could not get the tests db configuration

    try:
        with sqlite3.connect(sqlite_db_env) as conn:
            sqlite_db_create_reserved_ports_table(conn)
            sqlite_db_create_hpc_files_table(conn)
            sqlite_db = sqlite_db_env
            print('TestsInfoDB: Database file', sqlite_db)
        return True
    except Exception as e:
        #Failed to setup tests DB. Set sqlite_db None
        print(e)
        return False

def skip_if_no_db(_func=None, *, value_when_skipped=None):
    '''
    Decorator to skip the function when the setup of DB fails
    '''
    def decf(func):
        @functools.wraps(func)
        def f(*args, **kwargs):
            if not setup_tests_info_db():
                return value_when_skipped
            return func(*args, **kwargs)
        return f
    if _func is None:
        return decf
    else:
        return decf(_func)

@skip_if_no_db(value_when_skipped=False)
def is_port_reserved(port):
    with sqlite3.connect(sqlite_db) as conn:
        sqlite_db_remove_stopped_processes(conn)
        query = 'select count(*) from reserved_ports where port = ?'
        resp = conn.execute(query, (port,)).fetchall()
        return resp[0][0] > 0

@skip_if_no_db
def reserve_port(port):
    with sqlite3.connect(sqlite_db) as conn:
        query = 'insert into reserved_ports (port) values (?)'
        conn.execute(query, (port,))

@skip_if_no_db
def release_ports(ports):
    for port in ports:
        release_port(port)

@skip_if_no_db
def release_port(port):
    with sqlite3.connect(sqlite_db) as conn:
        print ('TestsInfoDB: Removing port', str(port))
        query = 'delete from reserved_ports where port = ?'
        conn.execute(query, (port,))

@skip_if_no_db
def list_reserved_ports():
    with sqlite3.connect(sqlite_db) as conn:
        port = []
        query = 'select port from reserved_ports'
        for row in conn.cursor().execute(query):
            port.append(row[0])
        return port

@skip_if_no_db
def add_reserved_process_port(port, pid, service_name=None):
    with sqlite3.connect(sqlite_db) as conn:
        sqlite_db_add_used_port(conn, port, pid=pid, service_name=service_name)

@skip_if_no_db
def add_reserved_container_port(port, container_name, service_name=None):
    with sqlite3.connect(sqlite_db) as conn:
        sqlite_db_add_used_port(conn, port, container_name=container_name, service_name = service_name)

@skip_if_no_db
def remove_process_ports(pid):
    if not pid:
        return None
    with sqlite3.connect(sqlite_db) as conn:
       sqlite_db_remove_process_ports(conn, pid)

@skip_if_no_db
def remove_container_ports(container_name):
    if not container_name:
        return None
    with sqlite3.connect(sqlite_db) as conn:
        ports_query = 'select port from reserved_ports where container_name = ?'
        cursor = conn.cursor()
        for row in cursor.execute(ports_query, (container_name,)):
            print ('TestsInfoDB: Removing port', str(row[0]), '(docker container name: {} )'.format(container_name), 'from reserved_ports')
        query = 'delete from reserved_ports where container_name = ?'
        conn.execute(query, (container_name,))

@skip_if_no_db
def get_hpc_report_files():
    with sqlite3.connect(sqlite_db) as conn:
        hpc_files = []
        query = 'select filename from hpc_files'
        for row in conn.cursor().execute(query):
            hpc_files.append(row[0])
        return hpc_files

@skip_if_no_db
def add_hpc_report_file(hpc_file):
    hpc_file = os.path.abspath(hpc_file)
    with sqlite3.connect(sqlite_db) as conn:
        query = 'insert or ignore into hpc_files (filename) values (?)'
        conn.execute(query, (hpc_file,))

def sqlite_db_add_used_port(conn, port, pid=None, container_name=None, service_name=None):
    query1 = '''
    insert or ignore into reserved_ports
    (port, process_id, container_name)
    values (?,?,?)
    '''
    conn.execute(query1, (port, pid, container_name))
    query2 = 'update reserved_ports set process_id=?, container_name=? where port=?'
    conn.execute(query2, (pid, container_name, port))
    if service_name:
        service_info = 'for ' + service_name
    else:
        service_info = ''
    print('TestsInfoDB: reserving port', port, service_info)

def sqlite_db_remove_stopped_processes(conn):
    cursor = conn.cursor()
    query = 'select process_id from reserved_ports where process_id is not null;'
    for row in cursor.execute(query):
        pid = row[0]
        if not psutil.pid_exists(pid):
            sqlite_db_remove_process_ports(conn, pid)

def sqlite_db_remove_process_ports(conn, pid):
    cursor = conn.cursor()
    ports_query = 'select port from reserved_ports where process_id = ?'
    for row in cursor.execute(ports_query, (pid,)):
        print ('TestsInfoDB: Removing port', str(row[0]), '(pid: {} )'.format(str(pid)), 'from reserved_ports')
    del_query = 'delete from reserved_ports where process_id = ?'
    conn.execute(del_query, (pid,))

def sqlite_db_create_reserved_ports_table(conn):
    conn.execute(
        '''CREATE TABLE if not exists RESERVED_PORTS
        ( port INT PRIMARY KEY NOT NULL
        , process_id INT
        , container_name TEXT
        ) ;'''
    )

def sqlite_db_create_hpc_files_table(conn):
    conn.execute(
        '''CREATE TABLE if not exists HPC_FILES
        ( filename TEXT PRIMARY KEY NOT NULL
        ) ;'''
    )
