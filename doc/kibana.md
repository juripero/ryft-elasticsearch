## Kibana installation
To install Kibana use following commands:
```
wget https://download.elastic.co/kibana/kibana/kibana-4.6.1-amd64.deb
sudo dpkg -i kibana-4.6.1-amd64.deb
```
## Kibana operation
Kibana registered in system as service daemon. To start kibana service do:
```
sudo service kibana start
```
To stop kibana service do:
```
sudo service kibana stop
```
Kibana configuration file located in `/opt/kibana/config/` or `/etc/kibana/` depending on insatlled version. By default kibana works with elasticsearch from local environment. [Official documentation](https://www.elastic.co/guide/en/kibana/current/settings.html) describes configuration properties, that can be changed according to your needs.
