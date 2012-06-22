#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=1000 operationcount=100000 recordcount=102500000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=1000 operationcount=100000 recordcount=102600000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=1000 operationcount=100000 recordcount=102700000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=1000 operationcount=100000 recordcount=102800000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=1000 operationcount=100000 recordcount=102900000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=2000 operationcount=100000 recordcount=103000000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=2000 operationcount=100000 recordcount=103100000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=2000 operationcount=100000 recordcount=103200000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=2000 operationcount=100000 recordcount=103300000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=2000 operationcount=100000 recordcount=103400000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=3000 operationcount=100000 recordcount=103500000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=3000 operationcount=100000 recordcount=103600000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=3000 operationcount=100000 recordcount=103700000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=3000 operationcount=100000 recordcount=103800000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=3000 operationcount=100000 recordcount=103900000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=4000 operationcount=100000 recordcount=104000000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=4000 operationcount=100000 recordcount=104100000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=4000 operationcount=100000 recordcount=104200000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=4000 operationcount=100000 recordcount=104300000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=4000 operationcount=100000 recordcount=104400000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=5000 operationcount=100000 recordcount=104500000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=5000 operationcount=100000 recordcount=104600000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=5000 operationcount=100000 recordcount=104700000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=5000 operationcount=100000 recordcount=104800000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-g.workload target=5000 operationcount=100000 recordcount=104900000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1