#!/bin/sh
HOSTS=rnd-nosql-cbnode-1,rnd-nosql-cbnode-2,rnd-nosql-cbnode-3,rnd-nosql-cbnode-4
DEFAULT_PORT=8091
CLUSTER_RAMSIZE=12288

USER_NAME=ycsb
USER_PASSWORD=Kl1b0xAxQ
BUCKET_NAME=default
BUCKET_TYPE=membase
BUCKET_RAMSIZE=${CLUSTER_RAMSIZE}
BUCKET_REPLICAS=1
