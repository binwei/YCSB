BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

# start mongod config server
$MONGO_HOME/bin/mongod --configsvr --dbpath $MONDO_DB_DIR --fork --rest --logpath $MONGO_LOG_DIR/mongodb-configsvr.log --logappend --nojournal --quiet