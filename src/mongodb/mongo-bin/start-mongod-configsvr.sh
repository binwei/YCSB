BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

DEFAULT_PORT=27019
DEFAULT_PARAMETERS="--fork --rest --logappend --nojournal --quiet"

function show_usage() {
    if [ -n "$1" ]
    then
        echo "$(basename $0): $1"
    fi
    echo -e "usage: $(basename $0) [-p port]"
}

port=$DEFAULT_PORT

while getopts ":p:" opt; do
  case $opt in
    p)
        port=$OPTARG
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

parameters="$DEFAULT_PARAMETERS"
if [ ! -z "$port" ]
then
  parameters="--port $port $parameters"
fi

dbpath="$MONDO_DB_DIR/mongod-configsvr-$port"
mkdir -p $dbpath

logpath="$MONGO_LOG_DIR/mongod-configsvr-$port.log"

# start mongod config server
$MONGO_HOME/bin/mongod --configsvr --dbpath $dbpath --logpath $logpath $parameters