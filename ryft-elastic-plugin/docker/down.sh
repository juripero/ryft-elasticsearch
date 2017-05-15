#!/bin/bash

# read -p "Enter cluster size: " cluster_size
cluster_size=3
image="es-t"

# stop and remove containers
for ((i=0; i<$cluster_size; i++)); do
    docker stop "$image$i"
    docker rm -f "$image$i"
done

# remove image
docker rmi -f "$image"