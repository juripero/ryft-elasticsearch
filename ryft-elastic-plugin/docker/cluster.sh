#!/bin/bash
set -e

read -p "Enter cluster size: " cluster_size
read -p "Enter storage path: " storage
# read -p "Enter node memory (mb): " memory
#cluster_size=3

memory=512

heap=$((memory/2))
image="es-t"
network="es-net"
cluster="cluster-ryft"

# rebuild artifacts
mkdir -p .tmp
if [ ! -f .tmp/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb ]; then
    aws s3 cp s3://ryft-images/fake-ryft-server-0.11.0-alpha-13-g1b57b9b_amd64.deb .tmp
fi
mvn -f ../pom.xml clean install
cp ../target/releases/ryft-elastic-plugin-*.zip .tmp
mvn -f ../../ryft-elastic-codec/pom.xml clean install
cp ../../ryft-elastic-codec/target/ryft-elastic-codec-*.jar .tmp

# rebuild image
if [ "$(docker images -q  $image)" ];then
    docker rmi $(docker images -q  $image)
fi
docker build -t $image .

# create bridge network
if [ ! "$(docker network ls --filter name=$network -q)" ];then
    docker network create $network
fi

# concat all nodes addresses
hosts=""
for ((i=0; i<$cluster_size; i++)); do
    hosts+="$image$i"
	[ $i != $(($cluster_size-1)) ] && hosts+=","
done

# starting nodes
for ((i=0; i<$cluster_size; i++)); do
    echo "Starting node $i"

    docker run -d -p 920$i:9200 \
        --name "$image$i" \
        --network "$network" \
        -v "$storage":/ryftone/ \
        -v "$PWD/config/elasticsearch.yml":/usr/share/elasticsearch/config/elasticsearch.yml \
        --cap-add=IPC_LOCK --ulimit nofile=65536:65536 --ulimit memlock=-1:-1 \
        --memory="${memory}m" --memory-swap="${memory}m" --memory-swappiness=0 \
		-e ES_HEAP_SIZE="${heap}m" \
        --restart unless-stopped \
        $image \
        -Des.node.name="$image$i" \
        -Des.cluster.name="$cluster" \
        -Des.network.host=_eth0_ \
        -Des.discovery.zen.ping.multicast.enabled=false \
        -Des.discovery.zen.ping.unicast.hosts="$hosts" \
        -Des.cluster.routing.allocation.awareness.attributes=disk_type \
        -Des.node.rack=dc1-r1 \
        -Des.node.disk_type=ssd \
        -Des.node.data=true \
        -Des.bootstrap.mlockall=true \
        -Des.threadpool.bulk.queue_size=500
done

# get cluster status
until curl -s "http://localhost:9200/_cat/health?h=status"; do
    echo "waiting for cluster to form"
    sleep 5
done

sleep 10
status="$(curl -s "http://localhost:9200/_cat/health?h=status")"
echo "cluster health status is $status"
