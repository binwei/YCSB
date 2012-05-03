BASE_DIR=`dirname $0`
. $BASE_DIR/mysql-sharded-env.sh

cd $MYSQL_HOME
scripts/mysql_install_db --no-defaults --datadir=$MYSQLD_DATA_DIR