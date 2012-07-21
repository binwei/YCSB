#!/bin/sh

CASSANDRA_VERSION=1.1.2

wget http://www.us.apache.org/dist/cassandra/$CASSANDRA_VERSION/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz
tar -xf ./apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz
rm ./apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz
sudo mv ./apache-cassandra-$CASSANDRA_VERSION /usr/lib
sudo ln -s /usr/lib/apache-cassandra-$CASSANDRA_VERSION /usr/lib/apache-cassandra

sudo mkdir -p /raid/var/log/cassandra /raid/var/lib/cassandra
sudo chown ec2-user /raid/var/log/cassandra /raid/var/lib/cassandra
sudo ln -s /raid/var/log/cassandra /var/log/cassandra
sudo ln -s /raid/var/lib/cassandra /var/lib/cassandra

# JNA libraries

cd /usr/lib/apache-cassandra/lib
wget https://github.com/twall/jna/blob/master/dist/platform.jar?raw=true
wget https://github.com/twall/jna/blob/master/dist/jna.jar?raw=true
wget https://github.com/twall/jna/blob/master/dist/linux-ia64.jar?raw=true


sudo killall -9 java
sudo rm -rf /raid/var/lib/cassandra/* /raid/var/log/cassandra/*
sudo mkdir -p /raid/var/log/cassandra /raid/var/lib/cassandra
df -h

sudo /usr/lib/apache-cassandra/bin/cassandra

# Verify cluster
/usr/lib/apache-cassandra/bin/nodetool -host localhost ring

/usr/lib/apache-cassandra/bin/cassandra-cli -host localhost -port 9160

create keyspace UserKeyspace
  with placement_strategy = 'SimpleStrategy'
  and strategy_options = {replication_factor : 1}
  and durable_writes = true;

use UserKeyspace;

create column family UserColumnFamily
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'UTF8Type'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'ALL'
  and compression_options = {'chunk_length_kb' : '64', 'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};