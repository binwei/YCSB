package com.yahoo.ycsb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

public class SharedNothingJdbcClient extends BaseJdbcClient {

    private static Random RANDOM = new Random();

    private Connection connection;

    @Override
    protected void initConnections() throws ClassNotFoundException, SQLException {
        super.initConnections();
        int index = RANDOM.nextInt(connections.size());
        connection = connections.get(index);
    }

    @Override
    protected Connection getConnection(QueryDescriptor descriptor) {
        return connection;
    }

    @Override
    protected QueryDescriptor createQueryDescriptor(QueryType type, String table, String key, int parameters) {
        return createQueryDescriptor(type, table, parameters);
    }

    protected QueryDescriptor createQueryDescriptor(QueryType type, String table, int parameters) {
        return new SharedNothingQueryDescriptor(type, table, parameters);
    }
}
