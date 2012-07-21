BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

DEFAULT_PORT=27017

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

router=$MONGO_ROUTER

# config shards
$MONGO_HOME/bin/mongo $router:$port/admin $BASE_DIR/js/init-sharding.js