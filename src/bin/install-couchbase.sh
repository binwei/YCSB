#!/bin/sh
# wget http://packages.couchbase.com/releases/1.8.0/couchbase-server-enterprise_x86_64_1.8.0.rpm
# sudo rpm - couchbase-server-enterprise_x86_64_1.8.0.rpm
# rm -rf couchbase-server-enterprise_x86_64_1.8.0.rpm

ps aux | grep couchbase | awk '{print($2)}' | sudo xargs kill -9
sudo rm -rf /raid/var/lib/couchbase/
sudo rpm --erase couchbase-server
wget http://builds.hq.northscale.net/latestbuilds/couchbase-server-community_x86_64_1.8.1-937-rel.rpm
sudo yum install -y openssl098e
sudo rpm -i couchbase-server-community_x86_64_1.8.1-937-rel.rpm
rm -rf couchbase-server-community_x86_64_1.8.1-937-rel.rpm
sudo mkdir -p /raid/var/lib/couchbase/data
sudo chown couchbase -R /raid/var/lib/couchbase/