#!/bin/sh
HOSTS=rnd-nosql-cbnode1,rnd-nosql-cbnode2,rnd-nosql-cbnode3,rnd-nosql-cbnode4
# PRIMARY_HOST=rnd-nosql-node1

DEFAULT_PORT=8091
CLUSTER_RAMSIZE=13000

USER_NAME=ycsb
USER_PASSWORD=Kl1b0xAxQ
BUCKET_NAME=default
BUCKET_TYPE=couchbase
BUCKET_RAMSIZE=${CLUSTER_RAMSIZE}
BUCKET_REPLICAS=1
