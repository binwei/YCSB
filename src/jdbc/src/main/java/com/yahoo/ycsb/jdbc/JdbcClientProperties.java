package com.yahoo.ycsb.jdbc;

public interface JdbcClientProperties {

    final String PROPERTY_DEFAULT = "";

    final String URL_PROPERTY = "jdbc.url";

    final String USERNAME_PROPERTY = "jdbc.username";

    final String PASSWORD_PROPERTY = "jdbc.password";

    final String DRIVER_CLASS_NAME_PROPERTY = "jdbc.driverClassName";

    final String PRIMARY_KEY_PROPERTY = "jdbc.primaryKey";

    final String PRIMARY_KEY_DEFAULT = "ycsb_key";

    final String COLUMN_PREFIX_PROPERTY = "jdbc.columnPrefix";

    final String COLUMN_PREFIX_DEFAULT = "field";
}
