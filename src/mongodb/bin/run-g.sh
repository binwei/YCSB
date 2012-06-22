#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mongodb-${MONGODB_VERSION}-g.workload target=1000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=2000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=3000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=4000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=5000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=6000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=7000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=8000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=9000 operationcount=100000"
           "profile=mongodb-${MONGODB_VERSION}-g.workload target=10000 operationcount=100000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads"