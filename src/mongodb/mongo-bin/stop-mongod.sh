BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

$MONGO_HOME/bin/mongo localhost/admin $BASE_DIR/js/mongod-stop.js
