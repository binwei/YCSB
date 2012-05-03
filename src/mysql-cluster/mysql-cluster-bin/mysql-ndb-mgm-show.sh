BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-cluster-env.sh

$MYSQL_CLUSTER_HOME/bin/ndb_mgm -e show