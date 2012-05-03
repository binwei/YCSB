#!/bin/bash
BIN_HOME=./../../bin

. $BIN_HOME/utils.sh
. ./workload-env.sh

$BIN_HOME/run-workload.sh -w "profile=mysql-sharded-${MYSQL_SHARDED_VERSION}-load.workload" -i 1

