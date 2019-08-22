## Running Tests

The easiest way to run the test suite is to do:

    $ scripts/dev.sh test

This should install python dependencies if required, and run in isolation.

## Running Tests Using `run_pytest_scenario.py`

run_pytest_scenario using a yaml test configuration as input.

For example, the following will run the default set of tests
```bash
$ python3 run_pytest_scenario.py -f - <<< EOF
graphqlEngine:
  withStackExec:
    stackExecArgs: []
postgres:
  withDocker:
    databases:
    - hgetest1
    - hgetest2
    image: circleci/postgres:11.3-alpine-postgis
pytest:
  extraArgs: []
scenario:
  auth:
    noAuth: {}
  name: default
EOF

```

If the above configuration is in a file, the tests can be run as
```
python3 run_pytest_scenario.py -f test_conf_file.yaml
```
### Postgres setup
Postgres configuration can be either
- A docker configuration, with postgres docker image and the databases to be run in it. For example
```yaml
  postgres:
    withDocker:
      image: circleci/postgres:11.3-alpine-postgis
      databases:
      - db1
      - db2
```
- Or list of Postgres database urls. For example
```yaml
  postgres:
    urls:
    - postgresql://user1@pass1:host1:5432/db1
    - postgresql://user1@pass1:host1:5432/db2
    - postgresql://user2@pass2:host2:5432/db3
```
### GraphQL Engine Setup
The configuration of GraphQL Engine can be
- Run using `stack exec`.
```yaml
  graphql-engine:
    withStackExec:
      stackExecArgs: []
```
- Run using graphql-engine executable
```yaml
  graphql-engine:
    withExecutable: path_to_executable
```
- Run using graphql-engine docker image
```yaml
  graphql-engine:
    withDocker:
      image: hasura/graphql-engine:v1.0.0-beta.3
```
### Test scenario setup
Test scenario includes setting up authentication, setting up scenario specific environment variables and arguments for GraphQL servers, and setting up arguments for pytest. A general Test scenario is as follows
```yaml
  scenario:
    hge:
      args: ['hge_arg1','hge_arg2',..]
      env:
        env1: value1
        env2: value2
    pytest:
      args: ['pytest_arg1','pytest_arg2',..]
    auth: { auth_conf }
```
#### Authentication
Authentication used can be one of the following
- No authentication
```yaml
  auth:
    noAuth: {}
```
- Only admin secet
```yaml
  auth:
    adminSecret:
      secret: my_admin_secret / None for random secret
```
- JWT base authentication
```yaml
  auth:
    jwt:
      stringified: True / False
      issuer: jwtIssuer / None
      audience: ListOfAudiences / Audience / None
```
- Webhook based authentication
```yaml
  auth:
    webhook:
      secure: True / False
      mode: get / post
```

## Running Tests With Tox

With [Tox](https://tox.readthedocs.io/en/latest/), tests can be run in isolated environments. It also makes defining the environments a lot easier.

Running default tests is as simple as running
```
$ tox
```
### How it works
tox executes the following command after setting the required environmental variables
```
$ python3 run_pytest_scenario
```
which takes care of setting up Postgres and GraphQL servers, and running the tests using pytest.

For the definition of test environments and commands, see the tox configuration file [tox.ini](tox.ini).

### Environemts

The default tox environment is `pgDocker11.3-hgeStackExec-default-noAuth`. So the command
```
$ tox -e pgDocker11.3-hgeStackExec-default-noAuth
```
is same as running `$ tox`

#### Factors
In a tox [environment](https://tox.readthedocs.io/en/latest/config.html#tox-environment-settings), the parts delimited by hyphen are known as its [factors](https://tox.readthedocs.io/en/latest/config.html#factors-and-factor-conditional-settings).
In the above mentioned environment,
- `pgDocker11.3` stands for Postgres DB run on docker with version 11.3 (For the list of supported Postgres docker factors, see `pddockerimages` section in [tox.ini](tox.ini))
- `hgeStackExec` stands for GraphQL server being run using `stack exec`.
- `default` denotes the default test scenario (For the list of supported test scenarios, see section `scenario` in [tox.ini](tox.ini))
- `noAuth` denotes there is no authentication used with GraphQL server (For the list of supported authentication factors, see section `auth` in [tox.ini](tox.ini))

So the command
```
$ tox -e pgDocker10.6-hgeStackExec-corsDomains
```
will run tests for cors domains with Postgres (version 10.6) running in a docker and GraphQL server run using `stack exec`.

### Running tests on multiple environments
To run cors domain and query logs tests, one may run
```
$ tox -e 'pgDocker10.6-hgeStackExec-{corsDomains,queryLogs}'
```

Here tox takes care of bash like curly braces expansion of the environments and executing them.

This is equivalent to running the following command
```
$ tox -e pgDocker10.6-hgeStackExec-corsDomains -e pgDocker10.6-hgeStackExec-queryLogs
```

## Tests Organization
- Tests are grouped as test modules => test classes => test methods
- At module level, the tests capture a particular type of operation, or a particular test scenario
- _Pytest_ _Fixtures_ are used to provide the contexts for tests

## Fixtures

 [Test fixtures](https://docs.pytest.org/en/latest/fixture.html) provide the baseline on which we can execute repeatable tests.

For the complete list of test fixtures, do

    $ pytest --fixtures

Following are some of the already defined fixtures that may be useful in writing tests.

### Main Context Fixture
- The Fixture `hge_ctx` provides the main context for the tests.
- The context includes urls of Postgres and the GraphQL servers
- It also captures the flags setup for different test scenarios.
- The most useful method of the fixure is `hge_ctx.check_query_f(file)`
   - The function runs the test configuration given in the input file
   - It also applies the authentication headers required depending on the configuration


### Query Specific Fixtures
- The cost of database operations are usually DDL operations >> mutations > select queries
- We reduce the number of DDL operations as far as possible for faster completion of tests.
- The setup and teardown files mentioned below hold the metadata query to be send during those operations

#### Select / Aggregate Queries
- The database schema and data remains intact during any of these tests
- To reduce DDL operations, with Fixture `per_class_db_state`, the db state setup is performed only in the beginning of the class and teardown only in the end
- Fixture per_class_db_state expects the following class attributes:
  - Either the variable `dir` which holds the directory in which `setup.yaml` and `teardown.yaml` resides
  - Or the list of setup files and teardown files in variables `setup_files` and `teardown_files` respectively (The value can be either a single filename or list of filenames).

#### Mutations
- Schema remains the same in these tests
- Schema change operations with Fixture db_schema_for_mutations are done only in the beginning and in the end of the class for faster test execution
- Fixture `db_schema_for_mutations` expects the following defined in the class:
   - either variable `dir` provides directory path in which files `schema_setup.yaml` and `schema_teardown.yaml` resides
   - or the list of setup files in `schema_setup_files` and the list of teardown files list in `schema_teardown_files`.
- The Fixture `db_data_for_mutations` performs data setup and teardown per test method.
   - The setup / teardown files are optional in this fixture
   - Expects either `dir` variable which provide the directory with `data_setup.yaml` and/or `data_teardown.yaml` files
   - Or variable `data_setup_files` which holds the list of data setup files , and / or variable `data_teardown_files` which provides the list of data teardown files


#### Metadata Operations
- These tests may modify both schema and data
- A database state setup and teardown is performed per test method using Fixture `per_method_db_state`
- Fixture per_method_db_state requires the same configuration as for the Fixture per_class_db_state above
- The Fixture does optimization for tests with non-200 status.
   - It assumes that the database state hasn't changed for the tests which with non-200 status
   - In such a scenario, it skips the teardown in the current test method, and the setup on the next method.

### Remote GraphQL Server
- Fixture `remote_gql_server` provides the remote GraphQL server
- Use this fixture for schema stitching tests
- The factory Fixture `remote_gql_url` takes the path as input and provides the full URL as output (for the current test thread).
  - Use this fixture when you need the full URL for the Remoge GraphQL server

### Event Triggers Webhook
- Fixture `evts_webhook` provides the webhook server for events
- Use this fixture for events trigger based tests
- Similar to the Fixture `remote_gql_url`, `evts_webhook_url` takes the path as input and provides as output the full URL
  - Use this fixture when you need the full URL for the event triggers webhook
