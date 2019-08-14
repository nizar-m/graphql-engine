## Running Tests

The easiest way to run the test suite is to do:

    $ scripts/dev.sh test

This should install python dependencies if required, and run in isolation.

## Tests Organization
- Tests are grouped as test modules => test classes => test methods
- At module level, the tests capture a particular type of operation, or a particular test scenario
- _Pytest_ _Fixtures_ are used to provide the contexts for tests

## Fixtures

 [Test fixtures](https://docs.pytest.org/en/latest/fixture.html) are used to provide the baseline on which we can execute repeatable tests.

For the complete list of test fixtures, execute

    $ pytest --fixtures

Following are some of the defined fixtures that may be useful in writing tests.

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
- To reduce DDL operations (Fixture `per_class_db_state`), the db state setup is performed only in the beginning of the class and teardown only in the end
- Fixture `per_class_db_state` expects the following class attributes:
  - Either the variable `dir` which holds the directory in which `setup.yaml` and `teardown.yaml` resides
  - Or the list of setup files and teardown files in variables `setup_files` and `teardown_files` respectively (The value can be either a single filename or list of filenames).

#### Mutations
- Schema remains the same in these tests
- Schema change operations (Fixture `db_schema_for_mutations`) are done only in the beginning and in the end of the class for faster test execution
- `db_schema_for_mutations` expects the following defined in the class:
   - either variable `dir` provides directory path in which files `schema_setup.yaml` and `schema_teardown.yaml` resides
   - or the list of setup files in `schema_setup_files` and the list of teardown files list in `schema_teardown_files`.
- The Fixture `db_data_for_mutations` performs data setup and teardown per test method.
   - The setup / teardown files are optional in this fixture
   - Expects either `dir` variable which provide the directory with `data_setup.yaml` and/or `data_teardown.yaml` files
   - Or variable `data_setup_files` which holds the list of data setup files , and / or variable `data_teardown_files` which provides the list of data teardown files


#### Metadata Operations
- These tests may modify both schema and data
- A database state setup and teardown is performed per test method (Fixture `per_method_db_state`)
- Fixture requires the same configuration as for the fixture `per_class_db_state` above
- The Fixture does optimize for tests with non-200 status.
   - It assumes that the database state hasn't changed for the tests which with non-200 status
   - In such a scenario, it skips the teardown in the current test method, and the setup on the next method.

### Remote GraphQL Server
- Fixture `remote_gql_server` provides the remote GraphQL server
- Use this fixture for schema stitching tests
- The factory Fixture `remote_gql_url` takes the path as input and provides the full URL as output (for the current test thread).
  - Use this fixture when you need the full URL for the Remoge GraphQL server

### Event Triggers Webhook
- Fixture `evts_webhook` provides the webhook server for events
- use this fixture for events trigger based tests
- Similar to the Fixture `remote_gql_url`, `evts_webhook_url` takes the path as input and provides as output the full URL
  - Use this above fixture when you need the full URL for the event triggers webhook
