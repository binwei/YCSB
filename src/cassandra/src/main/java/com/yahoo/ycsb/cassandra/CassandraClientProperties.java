package com.yahoo.ycsb.cassandra;

public interface CassandraClientProperties {
    
    final String CONNECTION_RETRIES_PROPERTY = "cassandra.connectionRetries";
    final String CONNECTION_RETRIES_PROPERTY_DEFAULT = "300";

    final String OPERATION_RETRIES_PROPERTY = "cassandra.operationRetries";
    final String OPERATION_RETRIES_PROPERTY_DEFAULT = "300";

    final String USERNAME_PROPERTY = "cassandra.username";
    final String PASSWORD_PROPERTY = "cassandra.password";

    final String COLUMN_FAMILY_PROPERTY = "cassandra.columnFamily";
    final String COLUMN_FAMILY_PROPERTY_DEFAULT = "UserColumnFamily";

    final String HOSTS_PROPERTY = "cassandra.hosts";
}
