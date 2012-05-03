#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mongodb-${MONGODB_VERSION}-f.workload target=0 operationcount=100000"

           "profile=mongodb-${MONGODB_VERSION}-f.workload target=100 operationcount=10000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=300 operationcount=30000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=500 operationcount=50000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=800 operationcount=80000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=1000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=1000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=1500 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-f.workload target=2000 operationcount=100000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads"