BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-cluster-env.sh

$MYSQL_CLUSTER_HOME/bin/ndb_mgmd -f $MYSQL_CLUSTER_HOME/conf/config.ini --initial --configdir=$MYSQL_CLUSTER_HOME/conf