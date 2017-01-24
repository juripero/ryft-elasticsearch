#Ryft elastic plugin installation

##Jenkins installation
To install ryft-elastic-plugin please use Jenkins server

**Ryft-elasticsearch-build-and-deploy** job builds ES Ryft codec and plugin from current project and deploys ElasticSearch 2.4.1 with Ryft integration tarball into Amazon S3 bucket.
Next it deploys that tarball into specified Ryft machine. So you just need to launch it with needed version and machine IP.

###Manual starting/stopping
ElasticSearch can be start running following command:
```bash
sudo start elasticsearch
```
It can be stop using:
```bash
sudo stop elasticsearch
```

##Docker installation
To install ryft-elastic-plugin please consider to do the folowing: [ES docker installation](https://github.com/getryft/elastic-search)

After completing installation of ES. Stop docker container if you started it previously. 
Clone this repository to build machine and build codec with plugin executing folowing command in the project directories:
 - ryft-elastic-codec
 - ryft-elastic-plugin

```bash
mvn clean install
```

As the result next files should be generated:

 - ryft-elastic-codec/target/ryft-elastic-codec-1.0.0-SNAPSHOT.jar
 - ryft-elastic-plugin/target/releases/ryft-elastic-plugin-0.0.1-SNAPSHOT.zip

Copy these files to '~/ELK/' folder of ryft server.
Inside '~/ELK' folder unzip file ryft-elastic-plugin-0.0.1-SNAPSHOT.zip

Copy next content to the file '~/ELK/docker-elk/docker-compose.yml':

```yml
elasticsearch:
  image: elasticsearch:2.4.1
  volumes:
    - ./elasticsearch/config/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml
    - /home/ryftuser/ELK/ryft-elastic-codec-1.0.0.jar:/usr/share/elasticsearch/lib/ryft-elastic-codec-1.0.0.jar
    - /home/ryftuser/ELK/lucene-codecs-5.5.2.jar:/usr/share/elasticsearch/lib/lucene-codecs-5.5.2.jar
    - /home/ryftuser/ELK/plugin-descriptor.properties:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/plugin-descriptor.properties
    - /home/ryftuser/ELK/ryft-elastic-plugin-1.0.0.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/ryft-elastic-plugin-1.0.0.jar
    - /home/ryftuser/ELK/jackson-annotations-2.8.0.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/jackson-annotations-2.8.0.jar
    - /home/ryftuser/ELK/jackson-databind-2.8.1.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/jackson-databind-2.8.1.jar
    - /home/ryftuser/ELK/netty-all-4.0.33.Final.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/netty-all-4.0.33.Final.jar
    - /home/ryftuser/ELK/disruptor-3.2.0.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/disruptor-3.2.0.jar
    - /home/ryftuser/ELK/ryft.elastic.plugin.properties:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/ryft.elastic.plugin.properties
    - /home/ryftuser/ELK/plugin-security.policy:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/plugin-security.policy
    - /var/log/elasticsearch:/usr/share/elasticsearch/logs
    - ./elasticsearch/config/logging.yml:/usr/share/elasticsearch/config/logging.yml
    - /tmp/elasticsearch:/host_tmp
    - /ryftone/elasticsearch:/data
  command: elasticsearch -Des.network.host=0.0.0.0 -Djna.tmpdir=/host_tmp/ -Des.path.data=/data -Des.index.refresh_interval=5s
  #command: elasticsearch
  #command droped: -Des.path.logs=/logs
  ports:
    - "9200:9200"
    - "9300:9300"

kibana:
  build: kibana/
  dns: 8.8.8.8
  volumes:
    - ./kibana/config/:/opt/kibana/config/
  ports:
    - "5601:5601"
  links:
    - elasticsearch
```

Now you are ready to start elastic search with codec and plugin. 
To setup initial plugin settings you have to execute next command:

```bash
curl -XPUT "http://<ryft-sever>:9200/ryftpluginsettings/def/1" -d'
{
  "ryft_integration_enabled": "false",
  "ryft_query_limit":"100"
}'
```

##RDF 

 In order to provide ability to search indexed data Ryft Elasticsearch search integration uses codec that stores data in JSON format. Each file has special extension:

 ```sh
 <segment_name>.<index_name>jsonfld
 ```

For these type of files folowing RDF should be created and installed via Ryft API:

```sh
# JSON File RDF
#
data_type = "JSON";
rdf_name = "Elasticsearch index RDF"
chunk_size_mb = 64;
file_glob = "*.*jsonfld";
record_path = ".";
 ```



##Troubleshooting 

In case of system failure restart elastic search using command in ~/ELK/docker-elk folder

```bash
sudo docker-compose stop
sudo docker-compose start
```

If you follow [Jenkins installation](#jenkins-installation) you should use **ryft-elasticsearch-deploy** job for restarting elastic search.

Attention all data that was indexed via ES would be deleted completely!
Remove all indices via next command:
```bash
 curl -XDELETE "http://<ryft-sever>:9200/_all"
```
