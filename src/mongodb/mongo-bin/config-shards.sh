BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

# config shards
$MONGO_HOME/bin/mongo rnd-nosql-node-router/admin $BASE_DIR/js/config-shards.js