BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

$MONGO_HOME/bin/mongod --dbpath $MONDO_DB_DIR --fork --logpath $MONGO_LOG_DIR/mongodb-data.log --logappend --rest --nojournal