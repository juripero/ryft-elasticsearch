FROM elasticsearch:2.4.1

MAINTAINER ryft

RUN useradd -ms /bin/bash ryftuser
RUN mkdir -m 777 /ryftone
RUN chown ryftuser:ryftuser /ryftone

RUN apt-get update && apt-get install -y supervisor jq
ADD files/supervisord.conf /etc/supervisord.conf

ADD files/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb /tmp
RUN dpkg -i --force-all /tmp/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb; exit 0
ADD files/create-fake-data.sh /tmp

ADD files/elasticsearch.yml /usr/share/elasticsearch/config
ADD files/ryft-elastic-plugin-1.0.0-SNAPSHOT.zip /tmp
RUN unzip /tmp/ryft-elastic-plugin-1.0.0-SNAPSHOT.zip -d /usr/share/elasticsearch/plugins/ryft-elastic-plugin/
RUN echo 'JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=4000,suspend=n"' >> /usr/share/elasticsearch/bin/elasticsearch.in.sh

ADD files/ryft-elastic-codec-1.0.0-SNAPSHOT.jar /tmp
RUN mv /tmp/ryft-elastic-codec-1.0.0-SNAPSHOT.jar /usr/share/elasticsearch/lib/
RUN mv /usr/share/elasticsearch/plugins/ryft-elastic-plugin/lucene-codecs-5.5.2.jar /usr/share/elasticsearch/lib/

EXPOSE 8765 4000
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
