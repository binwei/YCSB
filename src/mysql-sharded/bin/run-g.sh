#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

workloads=("profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=1000 operationcount=100000 recordcount=100000000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=1000 operationcount=100000 recordcount=100100000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=1000 operationcount=100000 recordcount=100200000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=1000 operationcount=100000 recordcount=100300000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=1000 operationcount=100000 recordcount=100400000"

           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=2000 operationcount=100000 recordcount=100500000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=2000 operationcount=100000 recordcount=100600000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=2000 operationcount=100000 recordcount=100700000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=2000 operationcount=100000 recordcount=100800000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=2000 operationcount=100000 recordcount=100900000"

           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=3000 operationcount=300000 recordcount=101000000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=3000 operationcount=300000 recordcount=101300000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=3000 operationcount=300000 recordcount=101600000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=3000 operationcount=300000 recordcount=101900000"
           "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-g.workload target=3000 operationcount=300000 recordcount=102200000")

workloads=$(array_join "${workloads[@]}")

$BIN_HOME/run-workload.sh -w "$workloads" -i 1