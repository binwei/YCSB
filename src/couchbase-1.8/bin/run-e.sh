#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=couchbase-${COUCHBASE_VERSION}-e.workload target=20 operationcount=2000"
           "profile=couchbase-${COUCHBASE_VERSION}-e.workload target=30 operationcount=3000"
           "profile=couchbase-${COUCHBASE_VERSION}-e.workload target=50 operationcount=5000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads"