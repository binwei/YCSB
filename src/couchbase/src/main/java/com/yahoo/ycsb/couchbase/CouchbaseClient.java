package com.yahoo.ycsb.couchbase;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.internal.OperationFuture;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class CouchbaseClient extends DB implements CouchbaseClientProperties {

	private static Logger log = LoggerFactory.getLogger(CouchbaseClient.class);

	private static final String QUALIFIED_KEY = "{0}-{1}";

	private static final ObjectMapper MAPPER = new ObjectMapper();

	protected com.couchbase.client.CouchbaseClient client;

	public String[] getHosts() throws DBException {
		String hosts = getProperties().getProperty(HOSTS_PROPERTY);
		if (hosts == null) {
			throw new DBException("Required property hosts missing for Couchbase");
		}
		return hosts.split(",");
	}

	public void init() throws DBException {
		try {
			client = connect();
		} catch (Exception e) {
			throw new DBException(e);
		}
	}

	private com.couchbase.client.CouchbaseClient connect() throws URISyntaxException, DBException, IOException {
		String bucket = getProperties().getProperty(BUCKET_PROPERTY, BUCKET_PROPERTY_DEFAULT);
		String user = getProperties().getProperty(USER_PROPERTY);
		String password = getProperties().getProperty(PASSWORD_PROPERTY);
		String timeoutStr = getProperties().getProperty(TIMEOUT_PROPERTY);
		int timeout = DEFAULT_TIMEOUT;
		try {
			if (timeoutStr != null && timeoutStr.length() > 0) {
				timeout = Integer.parseInt(timeoutStr);
			}
		} catch (Exception x) {
			log.error("Error parsing timeout value: " + timeoutStr, x);
		}
		List<URI> servers = new ArrayList<URI>();
		for (String address : getHosts()) {
			servers.add(new URI("http://" + address + ":8091/pools"));
		}
		CouchbaseConnectionFactoryBuilder connFactoryByilder = new CouchbaseConnectionFactoryBuilder();
		connFactoryByilder.setOpTimeout(timeout);
		CouchbaseConnectionFactory connectionFactory = connFactoryByilder.buildCouchbaseConnection(servers, bucket,
				user, password);
		com.couchbase.client.CouchbaseClient client = new com.couchbase.client.CouchbaseClient(connectionFactory);
		client.addObserver(new ConnectionObserver() {
			public void connectionLost(SocketAddress sa) {
				if (log.isInfoEnabled()) {
					log.info("Connection lost to " + sa.toString());
				}
			}

			public void connectionEstablished(SocketAddress sa, int reconnectCount) {
				if (log.isInfoEnabled()) {
					log.info("Connection established with " + sa.toString());
				}
				if (reconnectCount > 0) {
					if (log.isInfoEnabled()) {
						log.info("Reconnected count: " + reconnectCount);
					}
				}
			}
		});
		return client;
	}

	public void cleanup() throws DBException {
		client.shutdown(SHUTDOWN_TIMEOUT_MILLIS, MILLISECONDS);
	}

	public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		String qualifiedKey = createQualifiedKey(table, key);
		try {
			Object object = client.get(qualifiedKey);
			if (object instanceof String) {
				fromJson((String) object, fields, result);
				return OK;
			} else {
				return ERROR;
			}
		} catch (IOException e) {
			if (log.isErrorEnabled()) {
				log.error("Error encountered", e);
			}
			return ERROR;
		}
	}

	public int scan(String table, String startKey, int recordCount, Set<String> fields,
			List<Map<String, ByteIterator>> result) {
		throw new IllegalStateException("Riak scan is not supported");
	}

	public int update(String table, String key, Map<String, ByteIterator> values) {
		String qualifiedKey = createQualifiedKey(table, key);
		try {
			Object object = client.get(qualifiedKey);
			Map<String, ByteIterator> newValues = new HashMap<String, ByteIterator>();
			if (object instanceof String) {
				Map<String, ByteIterator> oldValues = new HashMap<String, ByteIterator>();
				fromJson((String) object, null, oldValues);
				newValues.putAll(oldValues);
			}
			newValues.putAll(values);
			String json = toJson(newValues);
			OperationFuture<Boolean> result = client.set(qualifiedKey, Integer.MAX_VALUE, json);
			if (result.get()) {
				return OK;
			} else {
				return ERROR;
			}
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Error updating record with key: " + qualifiedKey, e);
			}
			return ERROR;
		}
	}

	public int insert(String table, String key, Map<String, ByteIterator> values) {
		String qualifiedKey = createQualifiedKey(table, key);
		try {
			OperationFuture<Boolean> result = client.add(qualifiedKey, Integer.MAX_VALUE, toJson(values));
			return result.get() ? OK : ERROR;
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Error inserting value", e);
			}
			return ERROR;
		}
	}

	public int delete(String table, String key) {
		String qualifiedKey = createQualifiedKey(table, key);
		try {
			OperationFuture<Boolean> result = client.delete(qualifiedKey);
			return result.get() ? OK : ERROR;
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Error deleting value", e);
			}
			return ERROR;
		}
	}

	private String createQualifiedKey(String table, String key) {
		return MessageFormat.format(QUALIFIED_KEY, table, key);
	}

	private static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
		JsonNode json = MAPPER.readTree(value);
		boolean checkFields = fields != null && fields.size() > 0;
		for (Iterator<Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext();) {
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

	private static String toJson(Map<String, ByteIterator> values) throws IOException {
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

	public static void main(String[] args) {
		try {
			CouchbaseClient client = new CouchbaseClient();
			Properties props = new Properties();
			props.setProperty(HOSTS_PROPERTY, args[0]);
			props.setProperty(BUCKET_PROPERTY, "default");

			client.setProperties(props);
			client.init();

			Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			values.put("field1", new StringByteIterator("value1"));
			values.put("field2", new StringByteIterator("value2"));
			values.put("field3", new StringByteIterator("value3"));
			client.insert("default", "theKey", values);

			Map<String, ByteIterator> results = new HashMap<String, ByteIterator>();
			client.read("default", "theKey", null, results);
			Map<String, ByteIterator> valuesToUpdate = new HashMap<String, ByteIterator>();
			valuesToUpdate.put("field2", new StringByteIterator("value2-up"));
			valuesToUpdate.put("field4", new StringByteIterator("value4"));
			client.update("default", "theKey", valuesToUpdate);
			results.clear();
			client.read("default", "theKey", null, results);
			client.delete("default", "theKey");
			client.read("default", "theKey", null, results);
			//
			client.cleanup();
		} catch (Exception e) {
			log.error("YCSB client error", e);
			System.exit(0);
		}
	}
}
