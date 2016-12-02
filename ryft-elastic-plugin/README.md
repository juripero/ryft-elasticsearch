##Supported elastic search API requests:

###Match query syntax:
Full form:
```javascript
{
    "query": {
        "match": {
            "field_name": {
                "query": "search query",
                "operator": <"and","or">,
                "fuzziness": <"AUTO", 0, 1, 2>,
                "type": "phrase",
                "metric": <"FEDS", "FHS">
            }
        }
    }
}
```
Simplified form:
```javascript
{
    "query": {
        "match" : {
            "field_name": "search query"
        }
    }
}
```
| Property  | RYFT possible values | Elasticsearch possible values | Notes                                    |
|-----------|----------------------|-------------------------------|------------------------------------------|
| query     | string               | string                        | Search query.                            |
| operator  | <"and", "or">        | <"and", "or">                 | Default "or".                            |
| fuzziness | <"auto", Integer>    | <"AUTO", 0, 1, 2>             | Default 0.                               |
| metric    | <"FEDS", "FHS">      |                               | Available only for RYFT. Default "feds". |
| type      | "phrase"             | "phrase"                      | Change query type to phrase.             |

#####Simple fuzzy match query example:
We are looking for speaker "MARCELLUS" with one mistake in his name 
```javascript
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
```javascript
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
```javascript
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
```javascript
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
```javascript
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
User can rewrite ```match_phrase``` query as ```match``` query like:
```javascript
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
```javascript
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
###Search on all record fields
It is possible to do search request on all record fields using keyword ```_all```.
```javascript
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
###Boolean query syntax:

```must``` and ```must_not``` queries would be combined with ```AND``` operator ```should``` queries would be combined with operator ```OR``` 
```javascript
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

#####Boolean query example:
```javascript
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
Sections ```must```, ```must_not```, ```should``` can contain only one sub-query.
Queries in section ```should``` are taken into account if no queries in other sections exists (details see [here](https://www.elastic.co/guide/en/elasticsearch/guide/current/bool-query.html)).
```javascript
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
```javascript
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
((RECORD.title CONTAINS "brown") OR (RECORD.title NOT_CONTAINS "dog"))
```
We can control how many should clauses need to match by using the ```minimum_should_match```
```javascript
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
((RECORD.title CONTAINS "brown") AND (RECORD.title NOT_CONTAINS "dog")) OR 
((RECORD.title CONTAINS "brown") AND (RECORD.title NOT_CONTAINS "fox")) OR
((RECORD.title CONTAINS "fox") AND (RECORD.title NOT_CONTAINS "dog")))
```
###Plugin configuration
Plugin has several configuration levels: configuration file, settings index, query properties.
All configuration properties can be defined in config file and some properties can be overridden by settings index and/or query properties.

| Property                            | Meaning                               |
|-------------------------------------|---------------------------------------|
| ryft_rest_client_host               | RYFT service host                     |
| ryft_rest_client_port               | RYFT service port                     |
| ryft_rest_auth                      | RYFT auth string                      |
| ryft_request_processing_thread_num  | Thread number for request processing  |
| ryft_response_processing_thread_num | Thread number for response processing |
| ryft_query_limit                    | Results limit                         |
| ryft_integration_enabled            | Integration with RYFT                 |
| ryft_plugin_settings_index          | Settings index name                   |

```ryft_integration_enabled``` and ```ryft_query_limit``` properties are overridden by ```ryft_enabled``` and ```size``` query properties.
```javascript
{
    "query": {
        "match" : {
            "speaker": "MARCELLUS"
        }
    },
    "ryft_enabled": true,
    "size": 100
}
```
###Search on non-indexed files
RYFT plugin able to perform record search on non-indexed json files. To do this ```ryft``` property should be used. It accepts object with configuration parameters.

| Parameter                           | Meaning                               |
|-------------------------------------|---------------------------------------|
| enabled                             | The same as ```ryft_enabled```        |
| files                               | List of files to search               |
| format                              | Input data format                     |
```javascript
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
```http://<host>:<port>/search?query=(RECORD.Description CONTAINS "vehicle")&file=chicago.crimestat&mode=es&local=true&stats=true&format=xml&limit=10
```

Example to do fuzzy search on non indexed files

```javascript
{
    "query": {
         "match_phrase": {
          "Description": {
            "query": "reckles conduct",
            "metric": "Feds",
            "fuzziness": 3
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

```
http://<host>:<port>/search?query=(RECORD.Description CONTAINS FEDS("reckles conduct", DIST=2))&file=chicago.crimestat&mode=es&local=true&stats=true&format=xml&limit=10
```
