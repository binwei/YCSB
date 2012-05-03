BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-cluster-env.sh

$MYSQL_CLUSTER_HOME/bin/mysql -h localhost -P 5000 -u root