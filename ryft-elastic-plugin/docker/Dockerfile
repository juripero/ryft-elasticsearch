FROM elasticsearch:2.4.1

RUN /usr/share/elasticsearch/bin/plugin install --batch lmenezes/elasticsearch-kopf

COPY /ryft/ryft-elastic-codec-1.1.0.jar /usr/share/elasticsearch/lib/ryft-elastic-codec-1.1.0.jar
COPY /ryft/lucene-codecs-5.5.2.jar /usr/share/elasticsearch/lib/lucene-codecs-5.5.2.jar
COPY /ryft/ryft-elastic-plugin /usr/share/elasticsearch/plugins/ryft-elastic-plugin
