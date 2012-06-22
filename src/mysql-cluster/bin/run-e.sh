#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=2 operationcount=200 recordcount=102000000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=2 operationcount=200 recordcount=102002000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=2 operationcount=200 recordcount=102004000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=2 operationcount=200 recordcount=102006000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=2 operationcount=200 recordcount=102008000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=3 operationcount=300 recordcount=102010000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=3 operationcount=300 recordcount=102013000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=3 operationcount=300 recordcount=102016000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=3 operationcount=300 recordcount=102019000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=3 operationcount=300 recordcount=102022000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=5 operationcount=500 recordcount=102025000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=5 operationcount=500 recordcount=102030000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=5 operationcount=500 recordcount=102035000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=5 operationcount=500 recordcount=102040000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=5 operationcount=500 recordcount=102045000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=10 operationcount=1000 recordcount=102050000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=10 operationcount=1000 recordcount=102060000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=10 operationcount=1000 recordcount=102070000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=10 operationcount=1000 recordcount=102080000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-e.workload target=10 operationcount=1000 recordcount=102090000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1