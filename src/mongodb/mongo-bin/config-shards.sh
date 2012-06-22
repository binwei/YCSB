BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

# config shards
$MONGO_HOME/bin/mongo $MONGO_ROUTER/admin $BASE_DIR/js/config-shards.js