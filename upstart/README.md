#Kibana and ElasticSearch upstart manual

To use upstart feature with Kibana and ElasticSearch on machine you need to copy [kibana.conf](kibana.conf) and [elasticsearch.conf](elasticsearch.conf) into **/etc/init/** directory.

Finally, Kibana/ElasticSearch can be start running following command:
```bash
sudo start kibana
sudo start elasticsearch
```
It can be stop using:
```bash
sudo stop kibana
sudo stop elasticsearch
```
Note that you can stop process by following command if you've started it using **sudo start** command.