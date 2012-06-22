#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=2 operationcount=200 recordcount=100000000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=2 operationcount=200 recordcount=100002000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=2 operationcount=200 recordcount=100004000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=2 operationcount=200 recordcount=100006000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=2 operationcount=200 recordcount=100008000"

           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=5 operationcount=500 recordcount=100010000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=5 operationcount=500 recordcount=100013000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=5 operationcount=500 recordcount=100016000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=5 operationcount=500 recordcount=100019000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=5 operationcount=500 recordcount=100022000"

           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=10 operationcount=1000 recordcount=100025000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=10 operationcount=1000 recordcount=100030000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=10 operationcount=1000 recordcount=100035000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=10 operationcount=1000 recordcount=100040000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=10 operationcount=1000 recordcount=100045000"

           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=20 operationcount=1000 recordcount=100050000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=20 operationcount=1000 recordcount=100060000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=20 operationcount=1000 recordcount=100070000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=20 operationcount=1000 recordcount=100080000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-e.workload target=20 operationcount=1000 recordcount=100090000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1