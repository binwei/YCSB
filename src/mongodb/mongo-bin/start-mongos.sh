BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

DEFAULT_PORT=27017
DEFAULT_PARAMETERS="--fork --logappend"

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

parameters=$DEFAULT_PARAMETERS

if [ ! -z "$port" ]
then
  parameters="--port $port $parameters"
fi

logpath="$MONGO_LOG_DIR/mongos-$port.log"
configdb=$MONGO_CONFIGDB

# run mongos router with --configdb parameter indicating the location of the config database(s)
$MONGO_HOME/bin/mongos --configdb $configdb --logpath $logpath $parameters
