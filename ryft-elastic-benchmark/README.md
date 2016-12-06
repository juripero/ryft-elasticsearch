# Benchmarking

## Test scenario

[Gatling stress tool](http://gatling.io/#/) for each test generates configurable amount of virtual Elasticsearch user. 
Generated users send requests to Elasticsearch.
Request, that have been sent by users, are search queries that are supported by ryft-elastic-plugin and by Elasticsearch.
Each virtual user was made constant amount of requests.

## System overview

Elasticsearch was installed on one of the Ryft boxes. Elastic search index data was stored in ryftone file system.
Elasticsearch JVM opts: ```-Xms4g -Xmx4g -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError -XX:+DisableExplicitGC -Des.index.refresh_interval=5s```

## Benchmarking results

Detailed results formatted as [gatling reports](https://github.com/getryft/ryft-elasticsearch/tree/master/ryft-elastic-benchmark/results).

Queries are stored [here, in the configs](https://github.com/getryft/ryft-elasticsearch/blob/master/ryft-elastic-benchmark/src/main/resources/application.conf). All queries were limited to fetch only first 1000 results. That was made because of differences in search algorithm between Elasticsearch an Ryft search to prevent returning too much results. Sometimes especially with high value(more then 1) of fuzziness it's possible that Ryft box will fetch more results.

First column describes amount of user that are simultaneously sending requests to the ES.
Float values in table describes request rate per second in other words amount of queries processed in a second.
Symbol '-' means that some requests failed with timeout error, so the results for that cells are not representative. That can be explained by the nature of Ryft BOX. Current Ryft search implementation puts all search tasks into the queue and process them sequentially, but Elasticsearch shares tasks between some amount of threads and process all tasks in parallel. That's why after reaching some constant request per second rate (0.298 in our case) requests would reach timeout.

### Elasticsearch results
Searching original Elasticsearch. All queries use Fuzzy edit distance 2. Index consists of 5 fragments.

| Users count |  1Gb  |  5Gb  | 10GB  |
|-------------|-------|-------|-------|
|      1      | 0.847 | 0.478 | 0.469 |
|      5      | 2.874 | 2.242 | 1.055 |
|      10     | 4.717 | 3.623 | 2.538 |
|      20     | 5.319 | 3.717 | 3.333 |

### Elasticsearch with Ryft results
Searching Elasticsearch with Ryft integration enabled. All queries uses Fuzzy edit distance 2. Index consists of 5 fragments.

| Users count |  1Gb  |  5Gb  | 10GB  |
|-------------|-------|-------|-------|
|      1      | 0.231 | 0.078 | 0.044 |
|      5      | 0.374 |   -   |   -   |
|      10     | 0.374 |   -   |   -   |

Searching Elasticsearch with Ryft integration enabled. All queries uses Fuzzy edit distance 4. Index consists of 5 fragments.

| Users count |  1Gb  |  5Gb  | 10GB  |
|-------------|-------|-------|-------|
|      1      | 0.137 | 0.067 | 0.037 |
|      5      | 0.314 |   -   |   -   |
|      10     | 0.313 |   -   |   -   |
	
Searching Elasticsearch with Ryft integration enabled. All queries uses Fuzzy edit distance 4. Index consists of 1 fragment.

| Users count |  1Gb  |  5Gb  | 10GB  |
|-------------|-------|-------|-------|
|      1      | 0.179 | 0.072 | 0.038 |
|      5      | 0.346 |   -   |   -   |
|      10     | 0.354 |   -   |   -   |

## Summary

* Ryft shows better results with search on index that consist from 1 file, than with fragmented index. 
* Requests with higher fuzzy distance take more time. 
* Elasticsearch processes requests in parallel, and shows better results when several clients make requests simultaneously.
