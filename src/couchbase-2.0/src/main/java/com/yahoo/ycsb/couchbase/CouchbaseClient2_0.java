package com.yahoo.ycsb.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.memcached.MemcachedClientBase;
import com.yahoo.ycsb.workloads.RangeScanOperation;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

@SuppressWarnings({"NullableProblems"})
public class CouchbaseClient2_0 extends MemcachedClientBase implements CouchbaseClientProperties, RangeScanOperation {

    public static final String SCAN_DESIGN_DOCUMENT_NAME = "";

    public static final String SCAN_VIEW_NAME = "_all_docs";

    protected View scanView;

    protected final Logger log = LoggerFactory.getLogger(getClass());

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

    @Override
    public int scan(String table, String startKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        try {
            Query query = new Query();
            query.setIncludeDocs(true);
            query.setRangeStart(createQualifiedKey(table, startKey));
            query.setLimit(limit);
            scanQuery(query, result);
            return OK;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error performing scan starting with a key: " + startKey, e);
            }
            return ERROR;
        }
        // throw new UnsupportedOperationException("Scan not implemented");
    }

    @Override
    public int scan(String table, String startKey, String endKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        try {
            Query query = new Query();
            query.setIncludeDocs(true);
            query.setRangeStart(createQualifiedKey(table, startKey));
            query.setRangeEnd(createQualifiedKey(table, endKey));
            query.setLimit(limit);
            scanQuery(query, result);
            return OK;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error performing scan starting with a key: " + startKey, e);
            }
            return ERROR;
        }
        // throw new UnsupportedOperationException("Scan not implemented");
    }

    protected void scanQuery(Query query, List<Map<String, ByteIterator>> result) throws IOException {
        ViewResponse response = ((CouchbaseClient) client).query(createScanView(), query);
        for (ViewRow row : response) {
            Map<String, ByteIterator> documentAsMap = new HashMap<String, ByteIterator>();
            fromJson((String) row.getDocument(), null, documentAsMap);
            result.add(documentAsMap);
        }
    }

    protected View createScanView() {
        if (scanView == null) {
            scanView = ((CouchbaseClient) client).getView(SCAN_DESIGN_DOCUMENT_NAME, SCAN_VIEW_NAME);
        }
        return scanView;
    }
}
