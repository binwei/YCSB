package com.yahoo.ycsb.couchbase;

import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import net.spy.memcached.FailureMode;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings({"NullableProblems"})
public class CouchbaseClient1_8 extends DB implements CouchbaseClientProperties {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static final String QUALIFIED_KEY = "{0}-{1}";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    protected com.couchbase.client.CouchbaseClient client;

    private boolean checkOperationStatus;

    @Override
    public void init() throws DBException {
        try {
            client = createCouchbaseClient();
            String checkOperationStatusValue = getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY);
            checkOperationStatus = checkOperationStatusValue != null ?
                    Boolean.valueOf(checkOperationStatusValue) : CHECK_OPERATION_STATUS_DEFAULT;
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    protected com.couchbase.client.CouchbaseClient createCouchbaseClient() throws URISyntaxException, DBException, IOException {
        String bucket = getProperties().getProperty(BUCKET_PROPERTY, BUCKET_PROPERTY_DEFAULT);
        String user = getProperties().getProperty(USER_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);
        String timeoutValue = getProperties().getProperty(TIMEOUT_PROPERTY);
        int timeout = DEFAULT_TIMEOUT;
        try {
            if (timeoutValue != null && timeoutValue.length() > 0) {
                timeout = Integer.parseInt(timeoutValue);
            }
        } catch (NumberFormatException e) {
            log.error("Error parsing timeout value: " + timeoutValue, e);
        }
        String failureModeValue = getProperties().getProperty(FAILURE_MODE_PROPERTY);
        FailureMode failureMode = FAILURE_MODE_PROPERTY_DEFAULT;
        if (failureModeValue != null) {
            failureMode = FailureMode.valueOf(failureModeValue);
        }
        String readBufferSizeValue = getProperties().getProperty(READ_BUFFER_SIZE_PROPERTY);
        int readBufferSize;
        if (readBufferSizeValue != null) {
            readBufferSize = Integer.parseInt(readBufferSizeValue);
        } else {
            readBufferSize = READ_BUFFER_SIZE_DEFAULT;
        }
        CouchbaseConnectionFactoryBuilder connectionFactoryBuilder = new CouchbaseConnectionFactoryBuilder();
        connectionFactoryBuilder.setReadBufferSize(readBufferSize);
        connectionFactoryBuilder.setOpTimeout(timeout);
        connectionFactoryBuilder.setFailureMode(failureMode);
        List<URI> servers = new ArrayList<URI>();
        for (String address : getHosts()) {
            servers.add(new URI("http://" + address + ":8091/pools"));
        }

        CouchbaseConnectionFactory connectionFactory =
                connectionFactoryBuilder.buildCouchbaseConnection(servers, bucket, user, password);
        return new com.couchbase.client.CouchbaseClient(connectionFactory);
    }

    protected String[] getHosts() throws DBException {
        String hosts = getProperties().getProperty(HOSTS_PROPERTY);
        if (hosts == null) {
            throw new DBException("Required property hosts missing for Couchbase");
        }
        return hosts.split(",");
    }

    @Override
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            GetFuture<Object> future = client.asyncGet(createQualifiedKey(table, key));
            Object document = future.get();
            if (document != null) {
                fromJson((String) document, fields, result);
            }
            return OK;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error encountered", e);
            }
            return ERROR;
        }
    }

    @Override
    public int scan(String table, String startKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        throw new IllegalStateException("Range scan is not supported");
    }

    @Override
    public int update(String table, String key, Map<String, ByteIterator> values) {
        String qualifiedKey = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.replace(qualifiedKey, OBJECT_EXPIRATION_TIME, toJson(values));
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error updating record with key: " + qualifiedKey, e);
            }
            return ERROR;
        }
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        String qualifiedKey = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.add(qualifiedKey, OBJECT_EXPIRATION_TIME, toJson(values));
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error inserting value", e);
            }
            return ERROR;
        }
    }

    @Override
    public int delete(String table, String key) {
        String qualifiedKey = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.delete(qualifiedKey);
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error deleting value", e);
            }
            return ERROR;
        }
    }

    protected int getReturnCode(OperationFuture<Boolean> future) {
        if (checkOperationStatus) {
            return future.getStatus().isSuccess() ? OK : ERROR;
        } else {
            return OK;
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (client != null) {
            client.shutdown(SHUTDOWN_TIMEOUT_MILLIS, MILLISECONDS);
        }
    }

    protected static String createQualifiedKey(String table, String key) {
        return MessageFormat.format(QUALIFIED_KEY, table, key);
    }

    protected static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
        JsonNode json = MAPPER.readTree(value);
        boolean checkFields = fields != null && fields.size() > 0;
        for (Iterator<Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext(); ) {
            Entry<String, JsonNode> jsonField = jsonFields.next();
            String name = jsonField.getKey();
            if (checkFields && fields.contains(name)) {
                continue;
            }
            JsonNode jsonValue = jsonField.getValue();
            if (jsonValue != null && !jsonValue.isNull()) {
                result.put(name, new StringByteIterator(jsonValue.asText()));
            }
        }
    }

    protected static String toJson(Map<String, ByteIterator> values) throws IOException {
        ObjectNode node = MAPPER.createObjectNode();
        HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
        for (Entry<String, String> pair : stringMap.entrySet()) {
            node.put(pair.getKey(), pair.getValue());
        }
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
        MAPPER.writeTree(jsonGenerator, node);
        return writer.toString();
    }
}
