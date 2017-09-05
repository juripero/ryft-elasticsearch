## Ryft elasticsearch integration test
### Overview
Integration test consists from three test suites: 
 * [Smoke test](src/test/java/com/ryft/elasticsearch/integration/test/SmokeTest.java)
 * [Aggregation test](src/test/java/com/ryft/elasticsearch/integration/test/AggregationTest.java)
 * [Non-indexed search test](src/test/java/com/ryft/elasticsearch/integration/test/NonIndexedSearchTest.java)

All tests create test indices and/or files with test records. Format of test record is shown below:

|    Field    |   Type  |               Content               |
|-------------|---------|-------------------------------------|
| registered  | String  | "yyyy-MM-dd HH:mm:ss"               |
| ipv6        | String  | ipv6 address                        |
| ipv4        | String  | ipv4 address                        |
| about       | String  | random sentence                     |
| company     | String  | random company                      |
| lastName    | String  | random name                         |
| firstName   | String  | random name                         |
| eyeColor    | String  | {"green", "blue", "brown"}          |
| age         | Integer | random number in range (16, 65)     |
| balance_raw | String  | "$#,##"                             |
| balance     | Double  | random real number range (0, 10000) |
| isActive    | Boolean | {"true", "false"}                   |
| index       | Integer | sequential id                       |
| location    | String  | comma separated coordinates         |

### Building and running
Integration test depends on RYFT swagger client that is generated from [swagger.json](https://github.com/getryft/ryft-server/blob/development/static/swagger.json) before compilation. In order to build integration tests you have to set proper path to swagger.json in property `ryft-client-swagger-file`. By default it expects swagger.json in `../../ryft-server/static/swagger.json`.
By default maven only builds integration test and skips test running. It is necessary to set explicitly maven.test.skip=false to run tests. Test accepts several properties that allows to configure it:

| Parameter name    | Description                          | Default value    |
|-------------------|--------------------------------------|------------------|
| test.cluster      | comma-separated list of URLs to test | localhost:9300   |
| test.index        | test index name                      | integration-test |
| test.records      | number of records to generate        | 100              |
| test.delete-index | is delete test index after finish    | true             |

Examples of test running command:

`mvn -Dmaven.test.skip=false clean test`

Executes all tests with default settings

`mvn -Dmaven.test.skip=false -Dtest.index="testdata" -Dtest.delete-index=false -Dtest.records=1000 -Dtest="NonIndexedSearchTest" test`

Executes non-indexed search test on test file named `testdata.json` with 1000 generated records and leaves this file on RYFT after running.