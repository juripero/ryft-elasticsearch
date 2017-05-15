#!/bin/bash
read -p "Enter cluster size: " CLUSTER_SIZE

mkdir -p .tmp
if [ ! -f .tmp/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb ]; then
    aws s3 cp s3://ryft-images/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb .tmp
fi
mvn -f ../pom.xml clean install -DskipTests
cp ../target/releases/ryft-elastic-plugin-*.zip .tmp
mvn -f ../../ryft-elastic-codec/pom.xml clean install -DskipTests
cp ../../ryft-elastic-codec/target/ryft-elastic-codec-*.jar .tmp

CONTAINERS=$(docker ps -a | grep elasticsearch | awk '{print $1}')
if [ -n "$CONTAINERS" ]; then
  docker stop $CONTAINERS
  docker rm $CONTAINERS
fi
docker rmi ryft-elasticsearch
docker-compose up -d
docker-compose scale elasticsearch=$CLUSTER_SIZE
docker-compose logs -f
