#!/bin/bash
mkdir -p files
if [ ! -f files/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb ]; then
    aws s3 cp s3://ryft-images/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb files
fi
mvn -f ../pom.xml clean install
cp ../target/releases/ryft-elastic-plugin-1.0.0-SNAPSHOT.zip files
mvn -f ../../ryft-elastic-codec/pom.xml clean install
cp ../../ryft-elastic-codec/target/ryft-elastic-codec-1.0.0-SNAPSHOT.jar files

docker rm $(docker ps -a | grep ryft-elasticsearch | awk '{print $1}')
docker rmi ryft-elasticsearch
docker-compose up
