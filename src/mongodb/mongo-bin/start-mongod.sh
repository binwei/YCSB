#!/bin/bash
BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

EXIT_INVALID_OPTION=1
EXIT_ARGUMENT_REQUIRED=2

DEFAULT_PORT=27017
DEFAULT_ARGUMENTS="--fork --logappend --rest --nojournal"

port=$DEFAULT_PORT
arguments="$DEFAULT_ARGUMENTS"

function show_usage() {
    if [ -n "$1" ]
    then
        echo "$(basename $0): $1"
    fi
    echo -e "usage: $(basename $0) [-r replica set name] [-p port] [-a arguments]"
}

while getopts ":r:p:a:" opt; do
  case $opt in
    r)
        replica_set=$OPTARG
    ;;
    p)
        port=$OPTARG
    ;;
    a)
        arguments=$OPTARG
    ;;
    \?)
        (show_usage "invalid option -$OPTARG")
        exit $EXIT_INVALID_OPTION
    ;;
    :)
        (show_usage "option -$OPTARG requires an argument")
        exit $EXIT_ARGUMENT_REQUIRED
    ;;
  esac
done

if [ ! -z "$replica_set" ]
then
  arguments="--replSet $replica_set $arguments"
fi

if [ ! -z "$port" ]
then
  arguments="--port $port $arguments"
fi

dbpath="$MONDO_DB_DIR/mongod-$port"
mkdir -p $dbpath

logpath="$MONGO_LOG_DIR/mongod-$port.log"

$MONGO_HOME/bin/mongod --dbpath $dbpath --logpath $logpath $arguments