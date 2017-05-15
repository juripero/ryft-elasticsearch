FROM elasticsearch:2.4.1

MAINTAINER ryft

RUN useradd -ms /bin/bash ryftuser
RUN mkdir -m 777 /ryftone
RUN chown ryftuser:ryftuser /ryftone

RUN apt-get update && apt-get install -y supervisor
ADD config/supervisord.conf /etc/supervisord.conf

COPY .tmp/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb /tmp
RUN dpkg -i --force-all /tmp/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb; exit 0

ADD config/elasticsearch.yml /usr/share/elasticsearch/config
ADD .tmp/ryft-elastic-plugin-*.zip /tmp
RUN unzip $(ls /tmp/ryft-elastic-plugin*.zip | head -n 1) -d /usr/share/elasticsearch/plugins/ryft-elastic-plugin/
RUN echo 'JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=4000,suspend=n"' >> /usr/share/elasticsearch/bin/elasticsearch.in.sh

ADD .tmp/ryft-elastic-codec-*.jar /usr/share/elasticsearch/lib/
RUN mv /usr/share/elasticsearch/plugins/ryft-elastic-plugin/lucene-codecs-5.5.2.jar /usr/share/elasticsearch/lib/

RUN /usr/share/elasticsearch/bin/plugin install --batch lmenezes/elasticsearch-kopf

EXPOSE 8765 4000
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
