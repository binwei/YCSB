package com.yahoo.ycsb.cassandra;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class CassandraClient extends DB implements CassandraClientProperties {

    private static Random RANDOM = new Random();

    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

    protected int connectionRetries;
    protected int operationRetries;
    protected String columnFamily;

    protected TTransport transport;
    protected Cassandra.Client client;

    protected boolean debug = false;
    protected String table;
    protected Exception error;

    protected List<Mutation> mutations = new ArrayList<Mutation>();
    protected Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
    protected Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

    protected ColumnParent parent;

    public String[] getHosts() throws DBException {
        String hosts = getProperties().getProperty(HOSTS_PROPERTY);
        if (hosts == null) {
            throw new DBException("Required property hosts missing for Cassandra");
        }
        return hosts.split(",");
    }

    public void init() throws DBException {
        String []hosts = getHosts();
        columnFamily = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
        parent = new ColumnParent(columnFamily);

        connectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRIES_PROPERTY,
                CONNECTION_RETRIES_PROPERTY_DEFAULT));
        operationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRIES_PROPERTY,
                OPERATION_RETRIES_PROPERTY_DEFAULT));

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        String host = hosts[RANDOM.nextInt(hosts.length)];

        Exception exception = null;

        for (int retry = 0; retry < connectionRetries; retry++) {
            transport = new TFramedTransport(new TSocket(host, 9160));
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new Cassandra.Client(protocol);
            try {
                transport.open();
                exception = null;
                break;
            } catch (Exception e) {
                exception = e;
            }
        }
        if (exception != null) {
            System.err.println("Unable to connect to " + host + " after " + connectionRetries + " tries");
            System.out.println("Unable to connect to " + host + " after " + connectionRetries + " tries");
            throw new DBException(exception);
        }
        if (username != null && password != null) {
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put("username", username);
            credentials.put("password", password);
            AuthenticationRequest req = new AuthenticationRequest(credentials);
            try {
                client.login(req);
            } catch (Exception e) {
                throw new DBException(e);
            }
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    public void cleanup() throws DBException {
        transport.close();
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        if (!StringUtils.equals(this.table, table)) {
            try {
                client.set_keyspace(table);
                this.table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return ERROR;
            }
        }
        for (int i = 0; i < operationRetries; i++) {
            try {
                SlicePredicate predicate;
                if (fields == null) {
                    predicate = new SlicePredicate().setSlice_range(new SliceRange(EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, 1000000));

                } else {
                    List<ByteBuffer> fieldsList = new ArrayList<ByteBuffer>(fields.size());
                    for (String field : fields) {
                        fieldsList.add(ByteBuffer.wrap(field.getBytes("UTF-8")));
                    }
                    predicate = new SlicePredicate().setColumn_names(fieldsList);
                }
                List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, ConsistencyLevel.ONE);
                if (debug) {
                    System.out.print("Reading key: " + key);
                }
                Column column;
                String name;
                ByteIterator value;
                for (ColumnOrSuperColumn oneResult : results) {
                    column = oneResult.column;
                    name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                    value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());
                    result.put(name, value);
                    if (debug) {
                        System.out.print(name + "=>" + value);
                    }
                }
                if (debug) {
                    System.out.println();
                }
                return OK;
            } catch (Exception e) {
                error = e;
            }
        }
        error.printStackTrace();
        error.printStackTrace(System.out);
        return ERROR;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startKey    The record key of the first record to read.
     * @param limit The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value
     *                    pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startKey, int limit, Set<String> fields,
                    List<Map<String, ByteIterator>> result) {
        if (!StringUtils.equals(this.table, table)) {
            try {
                client.set_keyspace(table);
                this.table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return ERROR;
            }
        }

        for (int i = 0; i < operationRetries; i++) {
            try {
                SlicePredicate predicate;
                if (fields == null) {
                    predicate = new SlicePredicate().setSlice_range(new SliceRange(EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, 1000000));
                } else {
                    List<ByteBuffer> fieldsList = new ArrayList<ByteBuffer>(fields.size());
                    for (String s : fields) {
                        fieldsList.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
                    }
                    predicate = new SlicePredicate().setColumn_names(fieldsList);
                }
                KeyRange keyRange = new KeyRange().setStart_key(startKey.getBytes("UTF-8")).setEnd_key(new byte[]{}).setCount(limit);
                List<KeySlice> results = client.get_range_slices(parent, predicate, keyRange, ConsistencyLevel.ONE);
                if (debug) {
                    System.out.println("Scanning start key: " + startKey);
                }
                Map<String, ByteIterator> tuple;
                for (KeySlice oneResult : results) {
                    tuple = new HashMap<String, ByteIterator>();

                    Column column;
                    String name;
                    ByteIterator value;
                    for (ColumnOrSuperColumn oneColumn : oneResult.columns) {
                        column = oneColumn.column;
                        name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                        value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                        tuple.put(name, value);
                        if (debug) {
                            System.out.print(name + "=>" + value);
                        }
                    }
                    result.add(tuple);
                    if (debug) {
                        System.out.println();
                    }
                }
                return OK;
            } catch (Exception e) {
                error = e;
            }
        }
        error.printStackTrace();
        error.printStackTrace(System.out);
        return ERROR;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, Map<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table  the name of the table
     * @param key    The record key of the record to insert.
     * @param values a map of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        if (!StringUtils.equals(this.table, table)) {
            try {
                client.set_keyspace(table);
                this.table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return ERROR;
            }
        }

        for (int i = 0; i < operationRetries; i++) {
            if (debug) {
                System.out.println("Inserting key: " + key);
            }

            try {
                ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));
                Column column;
                ColumnOrSuperColumn columnOrSuperColumn;
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    column = new Column();
                    column.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
                    column.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
                    column.setTimestamp(System.currentTimeMillis());

                    columnOrSuperColumn = new ColumnOrSuperColumn();
                    columnOrSuperColumn.setColumn(column);

                    mutations.add(new Mutation().setColumn_or_supercolumn(columnOrSuperColumn));
                }

                mutationMap.put(columnFamily, mutations);
                record.put(wrappedKey, mutationMap);

                client.batch_mutate(record, ConsistencyLevel.ONE);

                mutations.clear();
                mutationMap.clear();
                record.clear();

                return OK;
            } catch (Exception e) {
                error = e;
            }
        }
        error.printStackTrace();
        error.printStackTrace(System.out);
        return ERROR;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key) {
        if (!StringUtils.equals(this.table, table)) {
            try {
                client.set_keyspace(table);
                this.table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return ERROR;
            }
        }

        for (int i = 0; i < operationRetries; i++) {
            try {
                client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
                        new ColumnPath(columnFamily),
                        System.currentTimeMillis(),
                        ConsistencyLevel.ONE);

                if (debug) {
                    System.out.println("Delete key: " + key);
                }

                return OK;
            } catch (Exception e) {
                error = e;
            }
        }
        error.printStackTrace();
        error.printStackTrace(System.out);
        return ERROR;
    }

    public static void main(String[] args) {
        CassandraClient client = new CassandraClient();
        Properties props = new Properties();
        props.setProperty("hosts", args[0]);
        client.setProperties(props);
        try {
            client.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put("age", new StringByteIterator("57"));
        values.put("name", new StringByteIterator("bradley"));
        values.put("color", new StringByteIterator("blue"));
        int code = client.insert("UserColumnFamily", "user", values);
        System.out.println("Result of insert: " + code);

        Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        Set<String> fields = new HashSet<String>();
        fields.add("age");
        fields.add("name");
        fields.add("color");
        code = client.read("UserColumnFamily", "bradley", fields, result);
        System.out.println("Result of read: " + code);
        for (String key : result.keySet()) {
            System.out.println("[" + key + "]=[" + result.get(key) + "]");
        }

        code = client.delete("UserColumnFamily", "bradley");
        System.out.println("Result of delete: " + code);
    }
}
