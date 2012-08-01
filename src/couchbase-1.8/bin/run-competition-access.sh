#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=(
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=1000 operationcount=900000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=3000 operationcount=2700000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=5000 operationcount=4500000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=7000 operationcount=6300000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=10000 operationcount=9000000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=12000 operationcount=10800000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=15000 operationcount=13500000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=17000 operationcount=15300000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=20000 operationcount=18000000"
           "profile=couchbase-${COUCHBASE_VERSION}-competition-access.workload target=21000 operationcount=18900000"
)
workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1