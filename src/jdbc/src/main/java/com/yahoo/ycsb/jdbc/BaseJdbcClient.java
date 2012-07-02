package com.yahoo.ycsb.jdbc;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.workloads.RangeScanOperation;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.yahoo.ycsb.Utils.parseInt;
import static com.yahoo.ycsb.jdbc.QueryType.*;
import static com.yahoo.ycsb.workloads.CoreWorkload.*;

public abstract class BaseJdbcClient extends DB implements RangeScanOperation, JdbcClientProperties {

    protected List<Connection> connections;
    protected ConcurrentMap<QueryDescriptor, PreparedStatement> statements;
    protected String primaryKey;
    protected String columnPrefix;
    protected String table;
    protected int fieldCount;

    protected abstract Connection getConnection(QueryDescriptor descriptor);

    protected abstract QueryDescriptor createQueryDescriptor(QueryType type, String table, String key, int parameters);

    /**
     * Initialize the database connection and set it up for sending requests to the database.
     * This must be called once per client.
     *
     * @throws DBException
     */
    @Override
    public void init() throws DBException {
        Properties properties = getProperties();
        primaryKey = properties.getProperty(PRIMARY_KEY_PROPERTY, PRIMARY_KEY_DEFAULT);
        columnPrefix = properties.getProperty(COLUMN_PREFIX_PROPERTY, COLUMN_PREFIX_DEFAULT);

        table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
        fieldCount = parseInt(getProperties().getProperty(FIELD_COUNT_PROPERTY), FIELD_COUNT_PROPERTY_DEFAULT);

        connections = new ArrayList<Connection>(3);
        statements = new ConcurrentHashMap<QueryDescriptor, PreparedStatement>();

        try {
            initConnections();
        } catch (ClassNotFoundException e) {
            System.err.println("Can't find driver class: " + e);
            throw new DBException(e);
        } catch (SQLException e) {
            System.err.println("Database operation error: " + e);
            throw new DBException(e);
        }
    }

    protected void initConnections() throws ClassNotFoundException, SQLException {
        Properties properties = getProperties();
        String urls = properties.getProperty(URL_PROPERTY, PROPERTY_DEFAULT);
        String username = properties.getProperty(USERNAME_PROPERTY, PROPERTY_DEFAULT);
        String password = properties.getProperty(PASSWORD_PROPERTY, PROPERTY_DEFAULT);
        String driverClassName = properties.getProperty(DRIVER_CLASS_NAME_PROPERTY);
        if (driverClassName != null) {
            Class.forName(driverClassName);
        }
        for (String url : urls.split(",")) {
            Connection connection = DriverManager.getConnection(url, username, password);
            // Since there is no explicit commit method in the DB interface, all
            // operations should auto commit.
            connection.setAutoCommit(true);
            connections.add(connection);
        }
        System.out.println("Using " + connections.size() + " nodes");
    }

    protected String createInsertQuery(QueryDescriptor descriptor) {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(descriptor.getTable());
        query.append(" VALUES(?");
        int parameters = descriptor.getParameters();
        for (int i = 0; i < parameters; i++) {
            query.append(",?");
        }
        query.append(");");
        return query.toString();
    }

    protected String createReadQuery(QueryDescriptor descriptor) {
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        query.append(descriptor.getTable());
        query.append(" WHERE ");
        query.append(primaryKey);
        query.append(" = ");
        query.append("?;");
        return query.toString();
    }

    protected String createDeleteQuery(QueryDescriptor descriptor) {
        StringBuilder query = new StringBuilder("DELETE FROM ");
        query.append(descriptor.getTable());
        query.append(" WHERE ");
        query.append(primaryKey);
        query.append(" = ?;");
        return query.toString();
    }

    protected String createUpdateQuery(QueryDescriptor descriptor) {
        StringBuilder query = new StringBuilder("UPDATE ");
        query.append(descriptor.getTable());
        query.append(" SET ");
        int parameters = descriptor.getParameters();
        for (int i = 1; i <= parameters; i++) {
            query.append(columnPrefix);
            query.append(i);
            query.append("=?");
            if (i < parameters) {
                query.append(", ");
            }
        }
        query.append(" WHERE ");
        query.append(primaryKey);
        query.append(" = ?;");
        return query.toString();
    }

    protected String createScanQuery(QueryDescriptor descriptor) {
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        query.append(descriptor.getTable());
        query.append(" WHERE ");
        query.append(primaryKey);
        query.append(" >= ");
        query.append("? LIMIT 0, ?;");
        return query.toString();
    }

    protected String createRangeScanQuery(QueryDescriptor descriptor) {
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        query.append(descriptor.getTable());
        query.append(" WHERE ");
        query.append(primaryKey);
        query.append(" >= ? AND ");
        query.append(primaryKey);
        query.append(" <= ? LIMIT 0, ?;");
        return query.toString();
    }

    protected String createQuery(QueryDescriptor descriptor) {
        String query = null;
        switch (descriptor.getType()) {
            case INSERT:
                query = createInsertQuery(descriptor);
                break;
            case READ:
                query = createReadQuery(descriptor);
                break;
            case UPDATE:
                query = createUpdateQuery(descriptor);
                break;
            case DELETE:
                query = createDeleteQuery(descriptor);
                break;
            case SCAN:
                query = createScanQuery(descriptor);
                break;
            case RANGE_SCAN:
                query = createRangeScanQuery(descriptor);
                break;
        }
        return query;
    }


    protected PreparedStatement createPreparedStatement(QueryDescriptor descriptor)
            throws SQLException {
        PreparedStatement statement = statements.get(descriptor);
        if (statement == null) {
            String query = createQuery(descriptor);
            Connection connection = getConnection(descriptor);
            statement = connection.prepareStatement(query);
            statements.putIfAbsent(descriptor, statement);
        }
        return statement;
    }

    protected Set<String> getResultSetColumns(Set<String> fields, ResultSet resultSet) throws SQLException {
        if (fields == null) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            fields = new HashSet<String>();
            for (int i = 1; i <= columnCount; i++) {
                fields.add(metaData.getColumnName(i));
            }
            fields.remove(primaryKey);
        }
        return fields;
    }

    @Override
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            QueryDescriptor descriptor = createQueryDescriptor(READ, table, key, 1);
            PreparedStatement statement = createPreparedStatement(descriptor);
            statement.setString(1, key);
            ResultSet resultSet = statement.executeQuery();
            if (!resultSet.next()) {
                resultSet.close();
                return ERROR;
            }
            Set<String> columns = getResultSetColumns(fields, resultSet);
            for (String column : columns) {
                result.put(column, new ByteArrayByteIterator(resultSet.getBytes(column)));
            }
            resultSet.close();
            return OK;
        } catch (SQLException e) {
            System.err.println("Error in processing read of table " + table + ": " + e);
            return ERROR;
        }
    }

    @Override
    public int scan(String table, String startKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        try {
            QueryDescriptor descriptor = createQueryDescriptor(SCAN, table, startKey, 2);
            PreparedStatement statement = createPreparedStatement(descriptor);
            statement.setString(1, startKey);
            statement.setInt(2, limit);
            ResultSet resultSet = statement.executeQuery();
            Set<String> columns = getResultSetColumns(fields, resultSet);
            for (int i = 0; i < limit && resultSet.next(); i++) {
                Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
                for (String column : columns) {
                    values.put(column, new ByteArrayByteIterator(resultSet.getBytes(column)));
                }
                result.add(values);
            }
            resultSet.close();
            return OK;
        } catch (SQLException e) {
            System.err.println("Error in processing scan of table: " + table + e);
            return ERROR;
        }
    }

    @Override
    public int scan(String table, String startKey, String endKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        try {
            QueryDescriptor descriptor = createQueryDescriptor(RANGE_SCAN, table, startKey, 2);
            PreparedStatement statement = createPreparedStatement(descriptor);
            statement.setString(1, startKey);
            statement.setString(2, endKey);
            statement.setInt(3, limit);
            ResultSet resultSet = statement.executeQuery();
            Set<String> columns = getResultSetColumns(fields, resultSet);
            while (resultSet.next()) {
                Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
                for (String column : columns) {
                    values.put(column, new ByteArrayByteIterator(resultSet.getBytes(column)));
                }
                result.add(values);
            }
            resultSet.close();
            return OK;
        } catch (SQLException e) {
            System.err.println("Error in processing scan of table: " + table + e);
            return ERROR;
        }
    }

    @Override
    public int update(String table, String key, Map<String, ByteIterator> values) {
        try {
            QueryDescriptor descriptor = createQueryDescriptor(UPDATE, table, key, values.size());
            PreparedStatement statement = createPreparedStatement(descriptor);
            int index = 1;
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                statement.setString(index++, entry.getValue().toString());
            }
            statement.setString(index, key);
            return statement.executeUpdate() == 1 ? OK : ERROR;
        } catch (SQLException e) {
            System.err.println("Error in processing update to table: " + table + e);
            return ERROR;
        }
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            QueryDescriptor descriptor = createQueryDescriptor(INSERT, table, key, values.size());
            PreparedStatement statement = createPreparedStatement(descriptor);
            statement.setString(1, key);
            int index = 2;
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                String field = entry.getValue().toString();
                statement.setString(index++, field);
            }
            return statement.executeUpdate() == 1 ? OK : ERROR;
        } catch (SQLException e) {
            System.err.println("Error in processing insert to table: " + table + e);
            return ERROR;
        }
    }

    @Override
    public int delete(String table, String key) {
        try {
            QueryDescriptor descriptor = createQueryDescriptor(DELETE, table, key, 1);
            PreparedStatement statement = createPreparedStatement(descriptor);
            statement.setString(1, key);
            return statement.executeUpdate() == 1 ? OK : ERROR;
        } catch (SQLException e) {
            System.err.println("Error in processing delete to table: " + table + e);
            return ERROR;
        }
    }

    @Override
    public void cleanup() throws DBException {
        try {
            for (Connection connection : connections) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("Error closing the connection " + e);
            throw new DBException(e);
        }
    }
}