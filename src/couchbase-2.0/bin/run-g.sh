#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=couchbase-${COUCHBASE_VERSION}-g.workload target=1000 operationcount=100000"
           "profile=couchbase-${COUCHBASE_VERSION}-g.workload target=2000 operationcount=100000"
           "profile=couchbase-${COUCHBASE_VERSION}-g.workload target=3000 operationcount=100000"
           "profile=couchbase-${COUCHBASE_VERSION}-g.workload target=4000 operationcount=100000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads"