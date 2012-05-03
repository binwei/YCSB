#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mongodb-${MONGODB_VERSION}-e.workload target=0 operationcount=10000"

           "profile=mongodb-${MONGODB_VERSION}-e.workload target=20 operationcount=2000"
           "profile=mongodb-${MONGODB_VERSION}-e.workload target=30 operationcount=3000"
           "profile=mongodb-${MONGODB_VERSION}-e.workload target=50 operationcount=5000"
           "profile=mongodb-${MONGODB_VERSION}-e.workload target=70 operationcount=7000"
           "profile=mongodb-${MONGODB_VERSION}-e.workload target=100 operationcount=10000"
           "profile=mongodb-${MONGODB_VERSION}-e.workload target=200 operationcount=10000"
           "profile=mongodb-${MONGODB_VERSION}-e.workload target=300 operationcount=10000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads"