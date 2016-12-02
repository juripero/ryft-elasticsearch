# Benchmarking

## Test scenario

[Gatling stress tool](http://gatling.io/#/) for each test generates configurable amount of virtual elasticsearch user. 
Generated users send requests to Elasticsearch.
Request, that have been sent by users, are search queries that are supported by ryft-elastic-plugin and by Elasticsearch.
Each virtual user was made constant amount of requests.

## System overview

Elasticsearch was installed on one of the Ryft boxes. Elastic search index data was stored in ryftone file system.
Elasticsearch JVM opts: -Xms4g -Xmx4g -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError -XX:+DisableExplicitGC -Des.index.refresh_interval=5s

##Benchmarking results:

Detailed results fromatted as [gatling reports](https://github.com/getryft/ryft-elasticsearch/tree/master/ryft-elastic-benchmark/results). \n
Queries are stored [here, in the configs](https://github.com/getryft/ryft-elasticsearch/blob/master/ryft-elastic-benchmark/src/main/resources/application.conf). All queries were limited to fetch only first 1000 results. That was made because of differences in search algo between Elasticsearch an Ryft seach to prevent returning too much results. Sometimes especially with high value(more then 1) of fuzziness it's possible that Ryft box will fetch more results.



###Test1 
Searching original Elasticsearch. All queries use Fuzzy edit distance 2.

| Users count |  1gb  | 6.6GB | 9.6GB | 12GB  |
|-------------|-------|-------|-------|-------|
|      1      | 0.847 | 0.478 | 0.469 | 0.595 |
|      5      | 2.874 | 2.242 | 1.055 | 2.941 |
|      10     | 4.717 | 3.623 | 2.538 | 3.378 |
|      20     | 5.319 | 3.717 | 3.333 | 5.025 |

###Test2 
Searching Elasticsearch with Ryft integration enbaled. All queries uses Fuzzy edit distance 2.

| Users count | 1GB   | 6.6GB | 9.6GB | 12GB  |
|-------------|-------|-------|-------|-------|
|      1      | 0.198 | 0.051 | 0.039 | 0.031 |
|      5      | 0.297 |   -   |   -   |   -   |
|      10     | 0.298 |   -   |   -   |   -   |
|      20     | 0.298 |   -   |   -   |   -   |

First column describes amount of user that are simultaneously sending requests to the ES.
Float values in table describes request rate per second in other words amount of queries processed in a second.
Symbol '-' means that some requests failed with timeout error, so the results for that cells are not representative. That can be explained by the nature of Ryft BOX. Current Ryft searh imlementation puts all search tasks into the queue and process them synchronyously, but Elasticsearch shares tasks between some amount of threads and process all tasks in parallel. That's why after reaching some constant request per second rate (0.298 in our case) requests would reach timeout.



