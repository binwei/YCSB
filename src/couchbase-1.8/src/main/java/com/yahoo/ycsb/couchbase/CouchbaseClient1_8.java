package com.yahoo.ycsb.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.memcached.MemcachedClientBase;
import net.spy.memcached.MemcachedClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SuppressWarnings({"NullableProblems"})
public class CouchbaseClient1_8 extends MemcachedClientBase implements CouchbaseClientProperties {

    @Override
    protected MemcachedClient createMemcachedClient() throws Exception {
        return createCouchbaseClient();
    }

    protected CouchbaseClient createCouchbaseClient() throws Exception {
        Properties properties = getProperties();
        String bucket = properties.getProperty(BUCKET_PROPERTY, BUCKET_PROPERTY_DEFAULT);
        String user = properties.getProperty(USER_PROPERTY);
        String password = properties.getProperty(PASSWORD_PROPERTY);

        CouchbaseConnectionFactoryBuilder connectionFactoryBuilder = new CouchbaseConnectionFactoryBuilder();
        initConnectionFactoryBuilder(connectionFactoryBuilder);
        List<URI> servers = new ArrayList<URI>();
        for (String address : getHosts(properties)) {
            servers.add(new URI("http://" + address + ":8091/pools"));
        }
        CouchbaseConnectionFactory connectionFactory =
                connectionFactoryBuilder.buildCouchbaseConnection(servers, bucket, user, password);
        return new com.couchbase.client.CouchbaseClient(connectionFactory);
    }

    protected static String[] getHosts(Properties properties) throws DBException {
        String hosts = properties.getProperty(HOSTS_PROPERTY);
        if (hosts == null) {
            throw new DBException("Required property hosts missing for Couchbase");
        }
        return hosts.split(",");
    }
}
