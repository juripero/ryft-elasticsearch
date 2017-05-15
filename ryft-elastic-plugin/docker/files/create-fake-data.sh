#!/bin/bash
echo $NODE_NAME > /tmp/nodename
rm -r /var/ryft/fake/*
mkdir /var/ryft/fake/all
echo '{
  "query": "-p g -q \\(.+\\) -f .+ -e  -v -l",
  "stat": "stat.txt",
  "data": "data.txt",
  "index": "index.txt"
}' > /var/ryft/fake/all/fake.json
echo 'Matches: 1
Total Bytes: 100
Duration: 1
Fabric Data Rate: 1.0 MB/sec' > /var/ryft/fake/all/stat.txt
echo "{\"nodename\": \"$NODE_NAME\"}" > /var/ryft/fake/all/data.txt
echo "/ryftone/$CLUSTER_NAME/nodes/0/indices/reddit/0/index/_0.redditjsonfld,0,21,-1" > /var/ryft/fake/all/index.txt
