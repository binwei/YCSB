BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

# config shards
$MONGO_HOME/bin/mongo $BASE_DIR/js/init-replica-set.js