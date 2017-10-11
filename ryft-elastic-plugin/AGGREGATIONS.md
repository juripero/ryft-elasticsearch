## Aggregations
Plugin supports all types of aggregations which are supported by Elasticsearch. In order to calculate aggregation over search result returned from RYFT service, plugin creates two requests. First request gets results from RYFT service. Received from RYFT data is saved into tempropary index. Second request executes all necessary aggregations over data from tempropary index in elasticsearch. Finally tempropary index is deleted.

Tempropary index fields mapping is the same as in original index where data is searched. For non-indexed search tempropary index mapping is created automatically using [Elasticsearch dynamic mapping feature](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/dynamic-mapping.html). It guesses types of fields and in most cases it creates index with correct datatypes. But for some specific cases you have to define datatype of certain fields implicitly; e.g. date or geo-point datatypes. See [custom mapping](README.md#custom-mapping) for more details.

The following snippet captures the basic structure of aggregations:
```
"aggregations" : {
    "<aggregation_name>" : {
        "<aggregation_type>" : {
            <aggregation_body>
        }
        [,"meta" : {  [<meta_data_body>] } ]?
        [,"aggregations" : { [<sub_aggregation>]+ } ]?
    }
    [,"<aggregation_name_2>" : { ... } ]*
}
```

#### Aggregation engine
Aggregations can be performed either by Elastic or by the Ryft Server. By default, Elasticsearch is used. To configure
which aggregations should be performed on Ryft Server, use the `ryft_aggregations_on_ryft_server` parameter in the 
configuration. Aggregation syntax remains the same for both engines. 

### Sum
A single-value metrics aggregation that sums up numeric values that are extracted from the aggregated documents. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-sum-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "balance_sum": {
      "sum": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "balance_sum" : {
      "value" : 202649.84999999998
    }
  }
}
```
### Max
A single-value metrics aggregation that keeps track and returns the maximum value among the numeric values extracted from the aggregated documents. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-max-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "max_balance": {
      "max": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "max_balance" : {
      "value" : 9961.01
    }
  }
}
```
### Min
A single-value metrics aggregation that keeps track and returns the minimum value among the numeric values extracted from the aggregated documents. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-min-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "min_balance": {
      "min": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "min_balance" : {
      "value" : 121.12
    }
  }
}
```
### Avg
A single-value metrics aggregation that computes the averbalance of numeric values that are extracted from the aggregated documents. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-avg-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "avg_balance": {
      "min": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "avg_balance" : {
      "value" : 5196.150000000001
    }
  }
}
```
### Value count
A single-value metrics aggregation that counts the number of values that are extracted from the aggregated documents. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-valuecount-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "count_balance": {
      "min": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "count_balance" : {
      "value" : 39
    }
  }
}
```
### Stats
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents. The stats that are returned consist of: `min`, `max`, `sum`, `count` and `avg`. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-stats-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "stats_balance": {
      "stats": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "stats_balance" : {
      "count" : 39,
      "min" : 121.12,
      "max" : 9961.01,
      "avg" : 5196.150000000001,
      "sum" : 202649.85
    }
  }
}
```
### Extended stats
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents. The `extended_stats` aggregations is an extended version of the [`stats`](#stats) aggregation, where additional metrics are added such as `sum_of_squares`, `variance`, `std_deviation` and `std_deviation_bounds`. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-extendedstats-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "extstats_balance": {
      "stats": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "extstats_balance" : {
      "count" : 39,
      "min" : 121.12,
      "max" : 9961.01,
      "avg" : 5196.150000000001,
      "sum" : 202649.85,
      "sum_of_squares" : 1.4260299402643003E9,
      "variance" : 9564895.440687189,
      "std_deviation" : 3092.7165147629016,
      "std_deviation_bounds" : {
        "upper" : 11381.583029525804,
        "lower" : -989.2830295258027
      }

    }
  }
}
```
### Geo bounds
A metric aggregation that computes the bounding box containing all `geo_point` values for a field. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-geobounds-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "geobounds_location": {
      "geo_bounds": {
        "field": "location"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "geobounds_location" : {
      "bounds" : {
        "top_left" : {
          "lat" : 50.95381795428693,
          "lon" : 30.008582957088947
        },
        "bottom_right" : {
          "lat" : 50.024906946346164,
          "lon" : 30.96765087917447
        }
      }
    }
  }
}
```
### Geo centroid
A metric aggregation that computes the weighted centroid from all coordinate values for a `geo_point` datatype field. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-geocentroid-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "centroid_location": {
      "geo_centroid": {
        "field": "location"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "centroid_location" : {
      "location" : {
        "lat" : 50.514193470948015,
        "lon" : 30.42715499893977
      }
    }
  }
}
```
### Percentiles
A multi-value metrics aggregation that calculates one or more percentiles over numeric values extracted from the aggregated documents. Percentiles show the point at which a certain percentage of observed values occur. For example, the 95th percentile is the value which is greater than 95% of the observed values. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-percentile-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "balance_percentiles": {
      "percentiles": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "balance_percentiles" : {
      "values" : {
        "1.0" : 140.1504,
        "5.0" : 188.363,
        "25.0" : 2983.665,
        "50.0" : 5304.64,
        "75.0" : 7654.99,
        "95.0" : 9761.427,
        "99.0" : 9887.4154
      }
    }
  }
}
```
### Percentile ranks
A multi-value metrics aggregation that calculates one or more percentile ranks over numeric values extracted from the aggregated documents. Percentile rank show the percentage of observed values which are below certain value. For example, if a value is greater than or equal to 95% of the observed values it is said to be at the 95th percentile rank. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-metrics-percentile-rank-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "balance_percentile_ranks": {
      "percentile_ranks": {
        "field": "balance",
        "values": [1000, 2000, 4000, 8000]
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "balance_percentile_ranks" : {
      "values" : {
        "1000.0" : 15.295656525161602,
        "2000.0" : 22.776057891656194,
        "4000.0" : 31.624993605137213,
        "8000.0" : 76.14551438692642
      }
    }
  }
}
```
### Date histogram
A multi-bucket aggregation similar to the histogram except it can only be applied on date values. Since dates are represented in elasticsearch internally as long values, it is possible to use the normal histogram on dates as well, though accuracy will be compromised. To read more about this aggregation refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-aggregations-bucket-datehistogram-aggregation.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "registered_histogram": {
      "date_histogram": {
        "field": "registered",
        "interval": "1y"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "registered_histogram" : {
      "buckets" : [ {
        "key_as_string" : "2014-01-01 00:00:00",
        "key" : 1388534400000,
        "doc_count" : 2
      }, {
        "key_as_string" : "2015-01-01 00:00:00",
        "key" : 1420070400000,
        "doc_count" : 14
      }, {
        "key_as_string" : "2016-01-01 00:00:00",
        "key" : 1451606400000,
        "doc_count" : 16
      }, {
        "key_as_string" : "2017-01-01 00:00:00",
        "key" : 1483228800000,
        "doc_count" : 7
      } ]
    }
  }
}
```
### Aggregation with metadata
You can associate a piece of metadata with individual aggregations at request time that will be returned in place at response time. Consider this example where we want to associate the color with our aggregation.
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "avg": {
      "age_avg": {
        "field": "age"
      },
      "meta": {
        "color": "green"
      }
    }
  },
  "ryft_enabled": false
}
```
Then that piece of metadata will be returned in place for our titles terms aggregation:
```json
{
  "aggregations" : {
    "age_avg" : {
      "meta" : {
        "color" : "green"
      },
      "value" : 41.282051282051285
    }
  }
}
```
### Aggregation with script
Elasticsearch allows to use scripts in aggregations of all types. In order to enable this feature add following line in `elasticsearch.yml`
```
script.engine.groovy.inline.aggs: on
```
To read more about this refer to [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/modules-scripting.html).

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "avg_corrected": {
      "avg": {
        "field": "balance",
        "script": {
          "lang": "groovy",
          "inline": "_value * correction",
          "params": {
            "correction": 1.2
          }
        }
      }
    },
    "avg": {
      "avg": {
        "field": "balance"
      }
    }
  },
  "ryft_enabled": true
}
```
Response:
```json
{
  "aggregations" : {
    "avg" : {
      "value" : 5196.15
    },
    "avg_corrected" : {
      "value" : 6235.380000000001
    }
  }
}
```
### Aggregations with custom mapping
For non-indexed search aggregations over fields with datatypes that can not be defined by dynamic mapping we have to use custom mapping. 

Example query:
```json
{
  "query": {
    "match": {
      "eyeColor": "green"
    }
  },
  "aggs": {
    "location_geobounds": {
      "geo_bounds": {
        "field": "location"
      }
    },
    "registered_histogram": {
      "date_histogram": {
        "field": "registered",
        "interval": "1y"
      }
    }
  },
  "ryft": {
    "enabled": true,
    "files": ["integration_test.json"],
    "format": "json",
    "mapping": {
      "location": {
        "type": "geo_point"
      },
      "registered": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss"
      }
    }
  }
}
```
Response:
```json
{
  "aggregations" : {
    "registered_histogram" : {
      "buckets" : [ {
        "key_as_string" : "2014-01-01 00:00:00",
        "key" : 1388534400000,
        "doc_count" : 2
      }, {
        "key_as_string" : "2015-01-01 00:00:00",
        "key" : 1420070400000,
        "doc_count" : 14
      }, {
        "key_as_string" : "2016-01-01 00:00:00",
        "key" : 1451606400000,
        "doc_count" : 16
      }, {
        "key_as_string" : "2017-01-01 00:00:00",
        "key" : 1483228800000,
        "doc_count" : 7
      } ]
    },
    "location_geobounds" : {
      "bounds" : {
        "top_left" : {
          "lat" : 50.98630093038082,
          "lon" : 30.067335907369852
        },
        "bottom_right" : {
          "lat" : 50.00750393606722,
          "lon" : 30.948312990367413
        }
      }
    }
  }
}
```