#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=100 operationcount=10000 recordcount=100000000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=100 operationcount=10000 recordcount=100010000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=100 operationcount=10000 recordcount=100020000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=100 operationcount=10000 recordcount=100030000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=100 operationcount=10000 recordcount=100040000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=300 operationcount=30000 recordcount=100050000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=300 operationcount=30000 recordcount=100080000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=300 operationcount=30000 recordcount=100110000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=300 operationcount=30000 recordcount=100140000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=300 operationcount=30000 recordcount=100170000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=500 operationcount=50000 recordcount=100200000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=500 operationcount=50000 recordcount=100050000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=500 operationcount=50000 recordcount=100300000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=500 operationcount=50000 recordcount=100350000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=500 operationcount=50000 recordcount=100400000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1000 operationcount=100000 recordcount=100450000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1000 operationcount=100000 recordcount=100550000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1000 operationcount=100000 recordcount=100650000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1000 operationcount=100000 recordcount=100750000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1000 operationcount=100000 recordcount=100850000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1500 operationcount=100000 recordcount=100950000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1500 operationcount=100000 recordcount=101050000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1500 operationcount=100000 recordcount=101150000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1500 operationcount=100000 recordcount=101250000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=1500 operationcount=100000 recordcount=101350000"

           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=2000 operationcount=100000 recordcount=101450000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=2000 operationcount=100000 recordcount=101550000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=2000 operationcount=100000 recordcount=101650000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=2000 operationcount=100000 recordcount=101750000"
           "profile=mysql-cluster-${MYSQL_CLUSTER_VERSION}-d.workload target=2000 operationcount=100000 recordcount=101850000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1