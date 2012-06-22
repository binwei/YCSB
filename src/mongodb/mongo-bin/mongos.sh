BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

# run mongos router with --configdb parameter indicating the location of the config database(s)
$MONGO_HOME/bin/mongos --configdb $MONGO_ROUTER --fork --logpath $MONGO_LOG_DIR/mongodb-router.log --logappend
