BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-cluster-env.sh

$MYSQL_CLUSTER_HOME/bin/ndbd -c $MYSQL_MANAGEMENT_NODE:1186