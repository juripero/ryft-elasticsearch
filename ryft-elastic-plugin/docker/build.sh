#!/bin/bash
read -p "Enter cluster size: " CLUSTER_SIZE

TMP_FOLDER="elasticsearch/.tmp"
RYFT_SERVER_DEB="fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb"
RYFT_ES_SRC_FOLDER="../../../ryft-elasticsearch/"
mkdir -p $TMP_FOLDER
if [ ! -f $TMP_FOLDER/$RYFT_SERVER_DEB ]; then
    aws s3 cp s3://ryft-images/$RYFT_SERVER_DEB $TMP_FOLDER
fi
if [ ! -f $TMP_FOLDER/ryftx ]; then
    aws s3 cp s3://ryft-images/ryftx $TMP_FOLDER
fi
mvn -f $RYFT_ES_SRC_FOLDER/ryft-elastic-plugin/pom.xml clean install -DskipTests
cp $RYFT_ES_SRC_FOLDER/ryft-elastic-plugin/target/releases/ryft-elastic-plugin-*.zip $TMP_FOLDER
mvn -f $RYFT_ES_SRC_FOLDER/ryft-elastic-codec/pom.xml clean install -DskipTests
cp $RYFT_ES_SRC_FOLDER/ryft-elastic-codec/target/ryft-elastic-codec-*.jar $TMP_FOLDER

docker rmi ryft-elasticsearch
docker-compose up -d
docker-compose scale elasticsearch=$CLUSTER_SIZE
docker-compose logs -f
docker-compose stop
CONTAINERS=$(docker ps -a | grep elasticsearch | awk '{print $1}')
if [ -n "$CONTAINERS" ]; then
  docker rm $CONTAINERS
fi
