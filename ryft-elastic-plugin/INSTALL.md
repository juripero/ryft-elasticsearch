#Ryft elastic plugin installation

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
    - /home/ryftuser/ELK/ryft-elastic-codec-1.0.0-SNAPSHOT.jar:/usr/share/elasticsearch/lib/ryft-elastic-codec-1.0.0-SNAPSHOT.jar
    - /home/ryftuser/ELK/lucene-codecs-5.5.2.jar:/usr/share/elasticsearch/lib/lucene-codecs-5.5.2.jar
    - /home/ryftuser/ELK/plugin-descriptor.properties:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/plugin-descriptor.properties
    - /home/ryftuser/ELK/ryft-elastic-plugin-0.0.1-SNAPSHOT.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/ryft-elastic-plugin-0.0.1-SNAPSHOT.jar
    - /home/ryftuser/ELK/jackson-annotations-2.8.0.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/jackson-annotations-2.8.0.jar
    - /home/ryftuser/ELK/jackson-databind-2.8.1.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/jackson-databind-2.8.1.jar
    - /home/ryftuser/ELK/netty-all-4.0.33.Final.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/netty-all-4.0.33.Final.jar
    - /home/ryftuser/ELK/disruptor-3.2.0.jar:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/disruptor-3.2.0.jar
    - /home/ryftuser/ELK/ryft.elastic.plugin.properties:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/ryft.elastic.plugin.properties
    - /home/ryftuser/ELK/plugin-security.policy:/usr/share/elasticsearch/plugins/ryft-elastic-plugin/plugin-security.policy
    - /var/log/elasticsearch:/usr/share/elasticsearch/logs
    - /home/ryftuser/ELK/logging.yml:/usr/share/elasticsearch/config/logging.yml
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
