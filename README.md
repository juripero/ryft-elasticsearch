##Supported elastic search API requests:

###Match query syntax:

```javascript
{
    "query": {
        "match" : {
           	 "field_name": {
		        "query":"search query",
		        "operator":<"and","or">
		        "fuzziness":[1..2] #supported by ES
		        "type":"phrase" #Looking for whole phrase in search query
		      }
        }
    }
}
```

###Simple fuzzy match query:

We are looking for speaker MARCELLUS with one mistake in his name 
```javascript
{
    "query": {
        "match" : {
           	 "speaker": {
		        "query":"MRCELLUS",
		        "fuzziness":1,
		      }
        }
    }
}
```

We are looking for famous: "To be, or not to be that: " with two mistakes 'comma' and 'that'  
The query below would be devided in couple of queries with AND operator in between.
```javascript
{
    "query": {
        "match" : {
           	 "text_entry": {
		        "query":     "To be or not to be",
		        "fuzziness": "3", 
		        "operator":"and" 
		      }
        }
    }
}
```

Resulting ryft query: 
```javascript
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
            "doc.text_entry":{
            "query":"To be or not to be",
            "fuzziness":"3"
            }
        }
    }
}
```

As the result we will get only one result.
User can rewrite match_phrase query as simple match query like:

```javascript
{
    "query": {
        "match" : {
           	 "text_entry": {
		        "query":     "To be or not to be: tht",
		        "fuzziness": "2",
		        "type":"phrase"
		      }
        }
    }
}'
```

###Boolean query syntax:

Must and Must not queries would be combined with AND operator 'should queries' would be combined with operator OR 
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

Resulting query: 

```javascript
(must1 AND must2 AND NOT mustNot1 AND NOT mustNot2) AND ( (should1 and should2) OR (should1 and should3) OR (should2 and should3) OR .. )
```

Boolean query example:
```javascript
{
  "query": {
    "bool" : {
      "must" : [
         {
          "match_phrase": { 
           "text_entry": {
              "query":     "To be or not to",
              "fuzziness": "2"
            }
          }
         },
         {
          "match":{
             "speaker":{
              "query":"HAMLET"
          }
         }
        }]
      }
    }
  }
}
```
