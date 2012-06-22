package com.yahoo.ycsb.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.IndexMapReduce;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.query.indexes.KeyIndex;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.Transport;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterClientFactory;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.http.HTTPRiakClientFactory;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.raw.pbc.PBRiakClientFactory;
import com.basho.riak.client.raw.query.indexes.BinRangeQuery;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.workloads.RangeScanOperation;
import org.codehaus.jackson.JsonNode;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.yahoo.ycsb.StringByteIterator.getByteIteratorMap;

/**
 * Specifies either JSON over HTTP or Riak Protocol Buffers
 * riak.transport=http|pb (transport to be used)
 * riak.dataproperty=meta|value (where to store user data in user meta map or in value bytes array field)
 * riak.pb.idleConnectionTTLMillis
 * riak.pb.connectionTimeoutMillis
 * riak.http.timeout
 */
@SuppressWarnings({"NullableProblems"})
public class RiakClient extends DB implements RangeScanOperation, RiakClientProperties {

    private static final byte[] EMPTY_VALUE = new byte[0];

    private static final String CONTENT_TYPE_JSON_UTF8 = "application/json;charset=UTF-8";

    private static final String CHARSET_NAME_UTF_8 = "UTF-8";

    private static final Charset CHARSET_UTF_8 = Charset.forName(CHARSET_NAME_UTF_8);

    private static Random random = new Random();

    private static final JSSourceFunction MAP_FUNCTION = new JSSourceFunction("function(v) { return [v]; }");

    private static final Map<Class<? extends Configuration>, RiakClientFactory> REGISTRY =
            new ConcurrentHashMap<Class<? extends Configuration>, RiakClientFactory>(4);

    private RiakConversionUtils conversionUtils = new RiakConversionUtils(CHARSET_UTF_8);

    private RawClient client;

    private RiakDataField dataField;

    static {
        REGISTRY.put(PBClientConfig.class, PBRiakClientFactory.getInstance());
        REGISTRY.put(HTTPClientConfig.class, HTTPRiakClientFactory.getInstance());
        REGISTRY.put(PBClusterConfig.class, PBClusterClientFactory.getInstance());
        REGISTRY.put(HTTPClusterConfig.class, HTTPClusterClientFactory.getInstance());
    }

    private String[] getHosts() {
        String hosts = getProperties().getProperty(HOSTS_PROPERTY);
        if (hosts == null) {
            throw new RiakClientException("Required property hosts missing for Riak");
        }
        return hosts.split(",");
    }

    @Override
    public void init() throws DBException {
        String userDataProperty = getProperties().getProperty(DATA_FIELD_PROPERTY);
        if (userDataProperty != null) {
            try {
                dataField = RiakDataField.valueOf(userDataProperty.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new RiakClientException(String.format("Property %1$s should be one of %2$s",
                        DATA_FIELD_PROPERTY, Arrays.asList(RiakDataField.values())));
            }
        } else {
            dataField = DATA_FIELD_DEFAULT;
        }

        String transportProperty = getProperties().getProperty(TRANSPORT_PROPERTY);
        Transport transport;
        if (transportProperty != null) {
            try {
                transport = Transport.valueOf(transportProperty.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new RiakClientException(String.format("Property %1$s should be one of %2$s",
                        TRANSPORT_PROPERTY, Arrays.asList(Transport.values())));
            }
        } else {
            transport = TRANSPORT_PROPERTY_DEFAULT;
        }
        String[] hosts = getHosts();
        String host = hosts[random.nextInt(hosts.length)];
        Configuration configuration = getConfiguration(transport, host);
        RiakClientFactory clientFactory = REGISTRY.get(configuration.getClass());
        try {
            client = clientFactory.newClient(configuration);
        } catch (IOException riak) {
            throw new RiakClientException(riak);
        }
    }

    private Configuration getConfiguration(Transport transport, String host) {
        Configuration configuration;
        switch (transport) {
            case PB:
                String idleConnectionTTLMillisValue = getProperties().getProperty(PB_IDLE_CONNECTION_TTL_MILLIS);
                int idleConnectionTTLMillis = idleConnectionTTLMillisValue != null ?
                        Integer.parseInt(idleConnectionTTLMillisValue) : PB_IDLE_CONNECTION_TTL_MILLIS_DEFAULT;
                String connectionTimeoutMillisValue = getProperties().getProperty(PB_CONNECTION_TIMEOUT_MILLIS);
                int connectionTimeoutMillis = connectionTimeoutMillisValue != null ?
                        Integer.parseInt(connectionTimeoutMillisValue) : PB_CONNECTION_TIMEOUT_MILLIS_DEFAULT;
                PBClientConfig.Builder pbBuilder = new PBClientConfig.Builder();
                pbBuilder.withHost(host);
                pbBuilder.withIdleConnectionTTLMillis(idleConnectionTTLMillis);
                pbBuilder.withConnectionTimeoutMillis(connectionTimeoutMillis);
                configuration =  pbBuilder.build();
                break;
            case HTTP:
                String timeoutValue = getProperties().getProperty(HTTP_TIMEOUT);
                int timeout = timeoutValue != null ?
                        Integer.parseInt(timeoutValue) : HTTP_TIMEOUT_DEFAULT;
                HTTPClientConfig.Builder httpBuilder = new HTTPClientConfig.Builder();
                httpBuilder.withHost(host);
                httpBuilder.withTimeout(timeout);
                configuration = httpBuilder.build();
                break;
            default:
                throw new RiakClientException("Transport is not supported: " + transport);
        }
        return configuration;
    }

    @Override
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            RiakResponse response = client.fetch(table, key);
            IRiakObject []objects = response.getRiakObjects();
            if (objects.length > 0) {
                fromRiakData(objects[0], fields, result);
            }
        } catch (IOException e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return ERROR;
        }
        return OK;
    }

    private void fromRiakData(IRiakObject object, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
        if (dataField == RiakDataField.META) {
            conversionUtils.fromRiakMeta(object, fields, result);
        } else {
            conversionUtils.fromRiakValue(object, fields, result);
        }
    }

    @Override
    public int scan(String table, String startKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        throw new RiakClientException("Riak scan is not supported");
    }

    @Override
    public int scan(String table, String startKey, String endKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> results) {
        try {
            IndexMapReduce scan = new IndexMapReduce(client, new BinRangeQuery(KeyIndex.index, table, startKey, endKey));
            scan.addMapPhase(MAP_FUNCTION);
            MapReduceResult scanResult = scan.execute();
            Collection<JsonNode> objectsAsNodes = scanResult.getResult(JsonNode.class);
            for (JsonNode objectAsNode : objectsAsNodes) {
                Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
                IRiakObject object = conversionUtils.toRiakObject(objectAsNode);
                fromRiakData(object, fields, result);
                results.add(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return ERROR;
        }
        return OK;
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(table, key);
            builder.withContentType(CONTENT_TYPE_JSON_UTF8);
            if (dataField == RiakDataField.META) {
                builder.withUsermeta(conversionUtils.toRiakMeta(values));
                builder.withValue(EMPTY_VALUE);
            } else {
                builder.withValue(conversionUtils.toRiakValue(values));
            }
            client.store(builder.build(), StoreMeta.empty());
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return ERROR;
        }
        return OK;
    }

    @Override
    public int update(String table, String key, Map<String, ByteIterator> values) {
        try {
            RiakResponse response = client.fetch(table, key);
            IRiakObject []objects = response.getRiakObjects();
            if (objects.length == 0) {
                return ERROR;
            }
            IRiakObject object = objects[0];
            Map<String, ByteIterator> newValues = new HashMap<String, ByteIterator>();
            fromRiakData(object, null, newValues);
            newValues.putAll(values);
            if (dataField == RiakDataField.META) {
                object.getMeta().putAll(conversionUtils.toRiakMeta(newValues));
                object.setValue(EMPTY_VALUE);
            } else {
                object.setValue(conversionUtils.toRiakValue(newValues));
            }
            client.store(object, StoreMeta.empty());
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return ERROR;
        }
        return OK;
    }

    @Override
    public int delete(String table, String key) {
        try {
            client.delete(table, key);
        } catch (IOException e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return ERROR;
        }
        return OK;
    }

    @Override
    public void cleanup() throws DBException {
        client.shutdown();
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream("/Users/tazija/Projects/nosql-research/repository/src/riak/workloads/a.workload"));

        RiakClient client = new RiakClient();
        client.setProperties(properties);
        client.init();
        String table = properties.getProperty("table");

        String key = "jane.doe";
        Map<String, String> user = new HashMap<String, String>();
        user.put("sex", "female");
        user.put("login", "jane.doe");
        user.put("email", "jane.doe@gmail.com");
        String startKey = key;

        client.insert(table, key, getByteIteratorMap(user));

        key = "john.doe";
        user = new HashMap<String, String>();
        user.put("sex", "male");
        user.put("login", "john.doe");
        user.put("email", "john.doe@gmail.com");

        client.insert(table, "john.doe", getByteIteratorMap(user));
        String endKey = key;

        Map<String, ByteIterator> readResult = new HashMap<String, ByteIterator>();
        client.read(table, key, null, readResult);
        System.out.println(String.format("[%1$s]->[%2$s]", key, readResult));

        List<Map<String, ByteIterator>> scanResult = new ArrayList<Map<String, ByteIterator>>();
        client.scan(table, startKey, endKey, 100, null, scanResult);
        client.cleanup();
    }
}