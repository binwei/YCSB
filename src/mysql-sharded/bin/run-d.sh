#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=100 operationcount=10000 recordcount=101450000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=100 operationcount=10000 recordcount=101460000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=100 operationcount=10000 recordcount=101470000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=100 operationcount=10000 recordcount=101480000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=100 operationcount=10000 recordcount=101490000"
            
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=300 operationcount=30000 recordcount=101500000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=300 operationcount=30000 recordcount=101530000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=300 operationcount=30000 recordcount=101560000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=300 operationcount=30000 recordcount=101590000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=300 operationcount=30000 recordcount=101620000"
            
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=500 operationcount=50000 recordcount=101650000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=500 operationcount=50000 recordcount=101700000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=500 operationcount=50000 recordcount=101750000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=500 operationcount=50000 recordcount=101800000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=500 operationcount=50000 recordcount=101850000"
            
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=1000 operationcount=100000 recordcount=101900000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=1000 operationcount=100000 recordcount=102000000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=1000 operationcount=100000 recordcount=102100000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=1000 operationcount=100000 recordcount=102200000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=1000 operationcount=100000 recordcount=102300000"
            
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=2000 operationcount=100000 recordcount=100000000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=2000 operationcount=100000 recordcount=100100000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=2000 operationcount=100000 recordcount=100200000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=2000 operationcount=100000 recordcount=100300000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-d.workload target=2000 operationcount=100000 recordcount=100400000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1