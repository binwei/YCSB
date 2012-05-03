BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-sharded-env.sh

$MYSQL_HOME/bin/mysql -h localhost -u root