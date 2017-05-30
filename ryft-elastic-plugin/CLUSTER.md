## Cluster support in Ryft Elastic plugin

Original Elasticsearch cluster mechanism builds and manages routing tables for each node. 
This table is a representation of the state and location of shards distributed across the cluster. 
Such routing table is provided by Elasticsearch and accessible from installed RYFT plugin. 
ES takes care of node discovery, sharding, routing and failover while the plugin only handles ryft-specific logic.

The plugin intercepts the search requests, and then accesses the information about shard location from the routing 
table to query all appropriate RYFT services. The plugin then compiles the final result to the client from the replies
of each individual RYFT services. 
This workflow can be described as:

- Client sends request to elasticsearch on any, this request is intercepted by RYFT plugin.
- RYFT plugin defines locations of requested index(indices) primary shards from routing table, composes and sends search 
requests to RYFT services. 
- RYFT services execute search on requested shards and return results. Results from all shards returned by RYFT 
services are united in the final result.
- Final result is returned to client.
	
Requests can be sent to any node in the network, no master or slave nodes are defined. 
New data can be added to any Elasticsearch node and it will be automatically distributed to other nodes.

### Cluster deployment

The ryft plugin can be installed and operate in cluster mode on any number of Elasticsearch nodes as long as the 
following conditions are met:

- ES has to be installed into Ryft/F1 with all ryft-specific tools (ryftx or ryftprim).
- ES cluster has to be configured (sample configuration can be found [here](src/main/resources/elasticsearch.yml))
- All paths to data must be the same across all nodes.
- Ryft plugin has to be installed on all nodes in the cluster

