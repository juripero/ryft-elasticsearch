#Data upload

[Elasticsearch suggests](https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html) to use bulk API to index huge amount of data. 
Main idea of bulk API is to prepare file with folowing structure and upload it to the ES node, which will parse it and index the data inside.

```javascript
...
{ action: { metadata }}\n
{ request body        }\n
{ action: { metadata }}\n
{ request body        }\n
...
```

Benchmarks of Elasticsearch integrations were performed on the Reddit sample dataset. The sample dataset was represented by 12GBs file with millions of JSON documents (each ends with new line symbol), to index such amount of data into Elasticsearch we devided the file into 1000 of ~10MB bulk files, as the result we got files with folowing structure:

```javascript
...
{ "index": { "_index": "reddit", "_type": "redditType" ,"_id":1 }}
{"author_flair_text":null,"controversiality":0,"parent_id":"t1_cyhssxj","edited":false,"subreddit":"AskReddit","body":"Hmm, didn't know that, but I mostly do this to comments I like, so if I go back and look for it, it's not deleted. ","id":"cyhudtv","distinguished":null,"retrieved_on":1454208310,"created_utc":1451607874,"link_id":"t3_3ywxbq","author_flair_css_class":null,"gilded":0,"stickied":false,"author":"Chaosfreak610","score":3,"ups":3,"subreddit_id":"t5_2qh1i"}
...
```

To process the file we used [Java program] (https://github.com/getryft/ryft-elasticsearch/blob/master/ryft-elastic-plugin/src/main/java/Main.java)

This java class accept input file and output file name as an argument. Output file name will be used as a prefix for all generated files.
The 3rd parameter used for name of generated index.
The 4th parameter used for amount of files to be generated.


##Using Main.java generator

* Copy the class to the folder on your disk.

* Compile the java file:

```sh
$ javac Main.java
```

* Run the class using:

```sh
$ java -cp . Main /ryftone/redditCF /ryftone/redditCF/bulk/file reddit 1000

```

As the result in the folder /ryftone/redditCF/bulk/ will be 1000 files named file{1..1000}.

* Upload files to elasticsearch using:

```sh
$ for i in {1..100}; do curl -XPOST localhost:9200/_bulk --data-binary "@file${i}"; done

```




