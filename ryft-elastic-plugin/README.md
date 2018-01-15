## Supported elastic search API requests:

### Match query syntax:
Full form:
```
{
    "query": {
        "match": {
            "field_name": {
                "query": "search query",
                "operator": "and",
                "fuzziness": <"AUTO", 0, 1, 2>,
                "type": "phrase",
                "metric": <"FEDS", "FHS">
            }
        }
    }
}
```
Simplified form:
```json
{
    "query": {
        "match" : {
            "field_name": "search query"
        }
    }
}
```
| Property  | RYFT possible values | Elasticsearch possible values | Notes                                    |
| --------- | -------------------- | ----------------------------- | ---------------------------------------- |
| query     | string               | string                        | Search query.                            |
| operator  | <"and", "or">        | <"and", "or">                 | Default "or".                            |
| fuzziness | <"auto", Integer>    | <"AUTO", 0, 1, 2>             | Default 0.                               |
| metric    | <"FEDS", "FHS">      |                               | Available only for RYFT. Default "feds". |
| type      | "phrase"             | "phrase"                      | Change query type to phrase.             |

Fuzziness AUTO will result in the following distances:

0 for strings of one or two characters

1 for strings of three, four, or five characters

2 for strings of more than five characters

##### Simple fuzzy match query example:
We are looking for speaker "MARCELLUS" with one mistake in his name 
```json
{
    "query": {
        "match" : {
            "speaker": {
                "query": "MRCELLUS",
                "fuzziness": 1,
                "metric": "fhs"
            }
        }
    }
}
```
Resulting RYFT query: 
```
(RECORD.speaker CONTAINS FHS("MRCELLUS", DIST=1))
```
In the case when fuzziness equal 0 request translates to the exact search query.
```json
{
    "query": {
        "match" : {
            "speaker": {
                "query": "MARCELLUS",
                "fuzziness": 0
            }
        }
    }
}
```
The same in simplified form
```json
{
    "query": {
        "match" : {
            "speaker": "MARCELLUS"
        }
    }
}
```
Resulting RYFT query: 
```
(RECORD.speaker CONTAINS "MARCELLUS")
```
We are looking for famous: "To be, or not to be that: " with two mistakes 'comma' and 'that'
The query below would be divided in couple of queries with AND operator in between.
```json
{
    "query": {
        "match" : {
            "text_entry": {
                "query": "To be or not to be tht",
                "fuzziness": "AUTO", 
                "operator": "and" 
            }
        }
    }
}
```
Resulting RYFT query: 
```
((RECORD.text_entry CONTAINS FEDS("To", DIST=0)) AND 
(RECORD.text_entry CONTAINS FEDS("be", DIST=0)) AND 
(RECORD.text_entry CONTAINS FEDS("or", DIST=0)) AND 
(RECORD.text_entry CONTAINS FEDS("not", DIST=1)) AND 
(RECORD.text_entry CONTAINS FEDS("to", DIST=0)) AND 
(RECORD.text_entry CONTAINS FEDS("tht", DIST=1)))
```
Phrase matching is different from simple match because it will try to find the whole phrase.
```json
{
    "query": {
        "match_phrase": {
            "text_entry": {
            "query": "To be or not to be",
            "fuzziness": "3"
            }
        }
    }
}
```
As the result we will get only one result.
User can rewrite `match_phrase` query as `match` query like:
```json
{
    "query": {
        "match" : {
            "text_entry": {
                "query": "To be or not to be",
                "fuzziness": "3",
                "type":"phrase"
            }
        }
    }
}
```
Resulting RYFT query: 
```
(RECORD.text_entry CONTAINS FEDS("To be or not to be", DIST=3))
```
Fuzzy search also possible to do in following way:
```json
{
    "query": {
        "fuzzy" : {
            "text_entry": {
                "value": "knight",
                "fuzziness": "2"
            }
        }
    }
}
```
Resulting RYFT query: 
```
(RECORD.text_entry CONTAINS FEDS("knight", DIST=2))
```
### Search on all record fields
It is possible to do search request on all record fields using keyword ```_all```.
```json
{
    "query": {
        "match_phrase" : {
            "_all": "To be or not to be"
        }
    }
}
```
Resulting RYFT query: 
```
(RECORD CONTAINS "To be or not to be")
```
### Boolean query syntax:

`must` and `must_not` queries would be combined with `AND` operator `should` queries would be combined with operator `OR` 
`
{
  "query": {
    "bool" : {
      	"must" : [ {match_query_must1}, {match_query_must2}, .. ],
      	"should":[ {match_query_should1}, {match_query_should2}, .. ],
      	"must_not":[ {match_query_mustNot1}, {match_query_mustNot2}, .. ],
      	"minimum_should_match": [1..N]
      }
    }
}
```

##### Boolean query example:
```json
{
    "query": {
        "bool": {
            "must": [
                {
                    "match_phrase": { 
                        "text_entry": {
                            "query": "To be or not to be",
                            "fuzziness": "2"
                        }
                    }
                },
                {
                    "match": {
                        "speaker": "HAMLET"
                    }
                }
            ]
        }
    }
}
```
Resulting RYFT query: 
```
((RECORD.text_entry CONTAINS FEDS("To be or not to be", DIST=2)) AND 
 (RECORD.speaker CONTAINS "HAMLET"))
```
Sections `must`, `must_not`, `should` can contain only one sub-query.
Queries in section `should` are taken into account if no queries in other sections exists or defined `minimum_should_match` value (details see [here](https://www.elastic.co/guide/en/elasticsearch/guide/current/bool-query.html)).
```json
{
    "query": {
        "bool": {
            "must":     { "match": { "title": "quick" }},
            "must_not": { "match": { "title": "lazy"  }},
            "should": [
                { "match": { "title": "brown" }},
                { "match": { "title": "dog"   }}
            ]
        }
    }
}
```
Resulting RYFT query: 
```
((RECORD.title CONTAINS "quick") AND (RECORD.title NOT_CONTAINS "lazy"))
```
```json
{
    "query": {
        "bool": {
            "should": [
                { "match": { "title": "brown" }},
                { "match": { "title": "dog"   }}
            ]
        }
    }
}
```
Resulting RYFT query: 
```
((RECORD.title CONTAINS "brown") OR (RECORD.title CONTAINS "dog"))
```
```json
{
    "query": {
        "bool": {
            "must":     { "match": { "title": "quick" }},
            "must_not": { "match": { "title": "lazy"  }},
            "should": [
                { "match": { "title": "brown" }},
                { "match": { "title": "dog"   }}
            ],
            "minimum_should_match": 1
        }
    }
}
```
Resulting RYFT query:
```
((RECORD.title CONTAINS "quick") AND (RECORD.title NOT_CONTAINS "lazy") AND ((RECORD.title CONTAINS "brown") OR (RECORD.title CONTAINS "dog")))
```
We can control how many should clauses need to match by using the `minimum_should_match`
```json
{
    "query": {
        "bool": {
            "should": [
                { "match": { "title": "brown" }},
                { "match": { "title": "fox"   }},
                { "match": { "title": "dog"   }}
            ],
            "minimum_should_match": 2 
        }
    }
}
```
Resulting RYFT query: 
```
((RECORD.title CONTAINS "brown") AND (RECORD.title CONTAINS "dog")) OR
((RECORD.title CONTAINS "brown") AND (RECORD.title CONTAINS "fox")) OR
((RECORD.title CONTAINS "fox") AND (RECORD.title CONTAINS "dog")))
```
### Wildcard query syntax
Wildcards are supported for wildcard term queries, and inside match and match_phrase queries. For both types, only the
`?` symbol is supported. There is no support for the `*` symbol.

##### Wildcards in match queries
For wildcard search to work in match and match_phrase queries, the wildcard symbol should be escaped. If it is not 
escaped, the it will be treated as a symbol to search for in the text.

Match phrase: 
```json
{
  "query": {
    "match_phrase": {
      "name": {
        "query": "J\"?\"n\"?\" Doe",
        "fuzziness": 1
      }
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.name CONTAINS FEDS("J"?"n"?" Doe", DIST=1))
```

Match:
```json
{
  "query": {
    "match": {
      "name": "J\"?\"n\"?\" Doe"
    }
  }
}
```

Resulting RYFT query:
```
((RECORD.name CONTAINS "J"?"n"?"") AND (RECORD.name CONTAINS "Doe"))
```

##### Wildcard queries
In wildcard term queries, the wildcard symbol can be used without escaping, it will still be treated as a wildcard.

Full form:
```json
{
  "query": {
    "wildcard": {
      "postcode": {
        "value": "?Z99 ??Z"
      }
    }
  }
}
```

Simplified form:
```json
{
  "query": {
    "wildcard": {
      "postcode": "?Z99 ??Z"
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.postcode CONTAINS ""?"Z99 "??"Z")
```


### Regex queries
Regex within query should be pcre2-compliant.

The pcre2 primitive is supported for RAW_TEXT only.  No RECORD based queries are supported at this time.

Full form:
```json
{
  "query": {
    "regexp": {
      "postcode": {
        "value": "W[0-9].+"
      }
    }
  }
}
```

Simplified form:
```json
{
  "query": {
    "regexp": {
      "postcode": "W[0-9].+"
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.postcode CONTAINS PCRE2("W[0-9].+"))
```

### Date-Time queries
Date format pattern specified according to rules described [here](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html). 

Default date format is “yyyy-MM-dd HH:mm:ss”.

Date format pattern should have consistent separator characters for date and time, 
because RYFT can search date and time with one type of separator.

It is only possible to have one format defined per query

The date-time data type can be used in term queries and in range queries.

Query:
```json
{
  "query": {
    "term": {
      "timestamp": {
        "value": "2014/01/01 07:00:00",
        "type": "datetime",
        "format": "yyyy/MM/dd HH:mm:ss"
      }
    }
  }
}
```
Resulting RYFT query:
```
((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS = 07:00:00)))
```

Query: 
```json
{
  "query": {
    "range" : {
      "timestamp" : {
        "gt" : "2014/01/01 07:00:00",
        "lt" : "2014/01/07 07:00:00",
        "type": "datetime",
        "format": "yyyy/MM/dd HH:mm:ss"
      }
    }
  }
}
```

Resulting RYFT query:
```
(((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS > 07:00:00))) 
OR (RECORD.timestamp CONTAINS DATE(2014/01/01 < YYYY/MM/DD < 2014/01/07)) 
OR ((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/07)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS < 07:00:00))))
```

### Numeric queries
Default values for “separator” and “decimal” are “,”, and “.”.
Accepted numbers described [here](https://github.com/getryft/ryft-server/blob/development/docs/searchsyntax.md#number-search)

Query:
```json
{
  "query": {
    "term": {
      "price": {
        "value": 20,
        "type": "number",
        "separator":",",
        "decimal":"."
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.price CONTAINS NUMBER(NUM = "20", ",", "."))

```
This query also supports a simplified syntax:
```json
{
  "query": {
    "term": {
      "price": 20
    }
  }
}
```

Range query: 
```json
{
  "query": {
    "range" : {
      "age" : {
        "gte" : -1.01e2,
        "lte" : "2000.12",
        "type": "number"
      }
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.age CONTAINS NUMBER("-1.01e2" <= NUM <= "2000.12", ",", "."))
```

Searching for multiple exact values: 
```json
{
  "query": {
    "term": {
      "price": [20, 30]
    }
  }
}
```

Resulting RYFT query:
```
((RECORD.price CONTAINS NUMBER(NUM = "20", ",", ".")) OR (RECORD.price CONTAINS NUMBER(NUM = "30", ",", ".")))
```
This syntax is only supported for numeric search.

### Currency queries
Default values for “currency”, “separator” and “decimal” are “$”, “,”, and “.”.

Query:
```json
{
  "query": {
    "term": {
      "price": {
        "value": 10000,
        "type": "currency"
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.price CONTAINS CURRENCY(CUR = "$10000", "$", ",", "."))
```

Query:
```json
{
  "query": {
    "term": {
      "price": {
        "value": "$100",
        "type": "currency",
        "currency":"$"
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.price CONTAINS CURRENCY(CUR = "$100", "$", ",", "."))
```

Range query: 
```json
{
  "query": {
    "range" : {
      "price" : {
        "gte" : 10,
        "lte" : 20,
        "type":"currency",
        "separator":",",
        "decimal":".",
        "currency":"%"
      }
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.price CONTAINS CURRENCY("%10" <= CUR <= "%20", "%", ",", "."))
```

### IPv4 queries

Query:
```json
{
  "query": {
    "term": {
      "ip_addr": {
        "value": "192.168.10.11",
        "type": "ipv4"
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.ip_addr CONTAINS IPV4(IP = "192.168.10.11"))
```

Mask search query:
```json
{
  "query": {
    "term": {
      "ip_addr": {
        "value": "192.168.0.0/16",
        "type": "ipv4"
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.ip_addr CONTAINS IPV4("192.168.0.0" <= IP <= "192.168.255.255"))
```

Range query: 
```json
{
  "query": {
    "range": {
      "ip_addr": {
        "gte": "192.168.1.0",
        "lt":  "192.168.2.0",
        "type": "ipv4"
      }
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.ip_addr CONTAINS IPV4("192.168.1.0" <= IP < "192.168.2.0"))
```


### IPv6 queries

Query:
```json
{
  "query": {
    "term": {
      "ip_addr": {
        "value": "2001::db8",
        "type": "ipv6"
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.ip_addr CONTAINS IPV6(IP = "2001::db8"))
```

Mask search query:
```json
{
  "query": {
    "term": {
      "ip_addr": {
        "value": "2001::db8/32",
        "type": "ipv6"
      }
    }
  }
}
```
Resulting RYFT query:
```
(RECORD.ip_addr CONTAINS IPV6("2001::" <= IP <= "2001:0:ffff:ffff:ffff:ffff:ffff:ffff"))
```

Range query: 
```json
{
  "query": {
    "range": {
      "ip_addr": {
        "gte": "2001::db8",
        "lt":  "2001::db9",
        "type": "ipv6"
      }
    }
  }
}
```

Resulting RYFT query:
```
(RECORD.ip_addr CONTAINS IPV6("2001::db8" <= IP < "2001::db9"))
```


### Filter queries

Filter queries can be used to further limit the results that are returned from a search query. Functionally, they work
as an extra set of AND queries.

Filter queries are also required for the proper work of timeseries datasets in Kibana.

Query:
```json
{"query": {
    "filtered": {
      "query": {
        "query": {
          "term": {
            "registered": {
              "format": "yyyy-MM-dd HH:mm:ss",
              "type": "datetime",
              "value": "2014-01-01 07:00:00"
            }
          }
        },
        "ryft_enabled": true
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "registered": {
                  "gte": 1338646255122,
                  "lte": 1496412655122,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  }}
```

Resulting RYFT query:
```
((((RECORD.registered CONTAINS DATE(YYYY-MM-DD = 2012-06-02)) AND (RECORD.registered CONTAINS TIME(HH:MM:SS >= 17:10:55))) OR (RECORD.registered CONTAINS DATE(2012-06-02 < YYYY-MM-DD < 2017-06-02)) OR ((RECORD.registered CONTAINS DATE(YYYY-MM-DD = 2017-06-02)) AND (RECORD.registered CONTAINS TIME(HH:MM:SS <= 17:10:55)))) AND ((RECORD.registered CONTAINS DATE(YYYY-MM-DD = 2014-01-01)) AND (RECORD.registered CONTAINS TIME(HH:MM:SS = 07:00:00))))
```



### Plugin configuration
Plugin has several configuration levels: configuration file, settings index, query properties.
All configuration properties can be defined in config file and some properties can be overridden by settings index and/or query properties.

| Property                            | Meaning                                                                        |
| ----------------------------------- | -------------------------------------------------------------------------------|
| ryft_rest_client_host               | RYFT service host                                                              |
| ryft_rest_client_port               | RYFT service port                                                              |
| ryft_rest_auth_login                | RYFT service login                                                             |
| ryft_rest_auth_password             | RYFT service password                                                          |
| ryft_request_processing_thread_num  | Thread number for request processing                                           |
| ryft_response_processing_thread_num | Thread number for response processing                                          |
| ryft_integration_enabled            | Integration with RYFT                                                          |
| ryft_plugin_settings_index          | Settings index name                                                            |
| ryft_disruptor_capacity             | Capacity of internal queue                                                     |
| ryft_rest_client_thread_num         | NETTY internal number of threads to access Ryft REST                           |
| ryft_aggregations_on_ryft_server    | Comma-separated list of aggregations that will be performed on the Ryft Server |
| es_result_size                      | Default value of results to return                                             |

To change property value using settings index you have to execute next call:

```bash
curl -XPUT "http://<ryft-url>:9200/ryftpluginsettings/def/1" -d'
{
  "ryft_integration_enabled": "false",
  "es_result_size":"100",
  "ryft_rest_client_host":"172.16.14.3",
  "ryft_rest_client_port":"8765"
  
}'
```
Also, it's possible to edit ryft.elastic.plugin.properties file.

`ryft_integration_enabled` and `es_result_size` properties are overridden by `ryft_enabled` and `size` query properties.
```json
{
    "query": {
        "match" : {
            "speaker": "MARCELLUS"
        }
    },
    "ryft_enabled": true,
    "size": 10
}
```
Elasticsearch `size` property sets number of results for return to client. If available results more than `size` plugin skips odd results.

##### Case sensitivity
By default, search is not case-sensitive. To configure this setting, the `ryft` property should be used. 
The following configuration parameters should be present:

| Parameter      | Meaning                                      |
| -------------- | -------------------------------------------- |
| enabled        | The same as `ryft_enabled`                   |
| case_sensitive | Should search be case-sensitive?(true/false) |

## Search on non-indexed files
### Record search
RYFT plugin is able to perform record search on non-indexed files. To do this `ryft` property should be used. 
The following configuration parameters should be present:

| Parameter | Meaning                                                       |
| --------- | ------------------------------------------------------------- |
| enabled   | The same as `ryft_enabled`                                    |
| files     | List of files to search                                       |
| format    | Input data format. Accepted values: JSON, XML, UTF8, RAW, CSV |
| mapping   | Data fields mapping properies                                 |
```json
{
    "query": {
        "match": {"Description": "vehicle"}
    },
    "ryft": {
        "enabled": true,
        "files": ["chicago.crimestat"],
        "format": "Xml"
    },
    "size": 10
}
```

Such search query produce following request to RYFT: 
`
http://<host>:<port>/search?query=(RECORD.Description CONTAINS "vehicle")&file=chicago.crimestat&local=false&stats=true&ignore-missing-files=true&cs=false&format=xml&stream=true
`
Example to do fuzzy search on non indexed files:

```json
{
    "query": {
         "match_phrase": {
            "Description": {
                "query": "reckles conduct",
                "metric": "Feds",   
                "fuzziness": 3
            }
          }
    },
    "ryft": {
        "enabled": true,
        "files": ["chicago.crimestat"],
        "format": "Xml"
    },
    "size": 1
}
```
Such search query produce following request to RYFT: 

`
http://<host>:<port>/search?query=(RECORD.Description CONTAINS FEDS("reckles conduct", DIST=3))&file=chicago.crimestat&local=false&stats=true&ignore-missing-files=true&cs=false&format=xml&stream=true
`
##### Custom mapping
Mapping property contains information of result records datatypes and can be useful for some data aggregations. It supports same syntax as [Elasticsearch do](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/indices-put-mapping.html).

```json
{
  "ryft": {
    "mapping": {
      "registered": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "location": {
        "type": "geo_point"
      }
    }
  }
}
```
```json
{
  "ryft": {
    "mapping": {
      "properties": {
        "registered": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss"
        },
        "location": {
          "type": "geo_point"
        }
      }
    }
  }
}
```
Also plugin supports simplified syntax for mapping:
```json
{
  "ryft": {
    "mapping": {
      "registered": "type=date,format=yyyy-MM-dd HH:mm:ss",
      "location": "type=geo_point"
    }
  }
}
```

### Raw text search
RAW_TEXT search allows searching in unstructured and unindexed text data. 

For RAW_TEXT search, the "format" parameter should be either “raw” or “utf8”. Name of field to search can be any string,
because its value is ignored (“_all” is preferred). RAW_TEXT search supports all implemented types of term search.

Example query:
```json
{
  "query": {
    "match": {
      "_all": {
        "query": "lorem ipsum",
        "fuzziness": 1,
        "width": 30
      }
    }
  },
  "ryft": {
    "enabled": true,
    "files": ["loremipsum.txt"],
    "format": "utf8"
  }
}
```
Resulting RYFT query:
```
((RAW_TEXT CONTAINS FEDS("lorem", WIDTH=30, DIST=1)) OR (RAW_TEXT CONTAINS FEDS("ipsum", WIDTH=30, DIST=1)))
```

Parameter “width” contains number of surrounding symbols or value “line”. Default value of “width” is 0.

In order for RAW_TEXT search to properly work with the `AND` operator, the "width" has to be "line". If the "width" is
different, it will be automatically converted to "line".