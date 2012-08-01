#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=1000 operationcount=3000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=3000 operationcount=9000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=5000 operationcount=15000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=7000 operationcount=20000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=10000 operationcount=30000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=12000 operationcount=40000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=15000 operationcount=50000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=17000 operationcount=60000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=20000 operationcount=70000000")
#workloads=("profile=mongodb-${MONGODB_VERSION}-competition-access.workload target=25000 operationcount=80000000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1