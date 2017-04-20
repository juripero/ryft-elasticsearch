## Logstash installation
To install Logstash use following commands:
```
wget https://artifacts.elastic.co/downloads/logstash/logstash-5.1.2.deb
sudo dpkg -i logstash-5.1.2.deb
```
Logstash requires Java 8 for operation. If other version of Java is used in your system by default, you should define `JAVA_HOME` with path to Java 8 for Logstash. To do this add following value into `/etc/init/logstash.conf` before script section:
```
env JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```
## Logstash operation
Logstash registered in system as service daemon. To start logstash service do:
```
sudo service logstash start
```
To stop logstash service do:
```
sudo service logstash stop
```
According to folder layout from official documentation by default logstash reads pipeline configuration files from `/etc/logstash/conf.d` folder. [Pipeline configuration](pipeline.conf) consists from three parts: input, filter, output. Input section describes how to get data. Filter section contains steps necessary for data processing. Last part declares how to output processed data. We should use logstash plugins to make necessary actions on every step of pipeline.

Logstash [file input plugin](https://www.elastic.co/guide/en/logstash/current/plugins-inputs-file.html) is used in input section to read information from log file. This plugin tracks current position in each file by recording it in a separate file named sincedb. This makes it possible to stop and restart Logstash and have it pick up where it left off without missing the lines that were added to the file while Logstash was stopped. You should delete sincedb files to make logstash process files from beginning again.
```
rm -f /tmp/*.sincedb
```
Filter sections use plugins [grok](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html), [date](https://www.elastic.co/guide/en/logstash/current/plugins-filters-date.html), [mutate](https://www.elastic.co/guide/en/logstash/current/plugins-filters-mutate.html) and [drop](https://www.elastic.co/guide/en/logstash/current/plugins-filters-drop.html) to match log lines by predefined regular expressions, parse datetime field, drop non parsed lines and some odd fields in order to make parsed data pretty. Custom patterns contains in folder `/etc/logstash/patterns`.

Output section writes parsed data to corresponding elasticsearch index grouped by date. Output plugin [elasticsearch](https://www.elastic.co/guide/en/logstash/current/plugins-outputs-elasticsearch.html) is used here.
You can [list all indices](https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html#cat-indices) from elasticsearch to ensure that logstash works.

To be able to search on these indices you should not import these indices into kibana as time-based, because ryft-plugin doesn’t support search on date range on this moment.
