BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-cluster-env.sh

# --defaults-file instructs to use the given option file only. If the file does not exist, the program exits with an error.
$MYSQL_CLUSTER_HOME/bin/mysqld --defaults-file=$MYSQL_CLUSTER_HOME/conf/my.cnf &