package com.yahoo.ycsb.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.memcached.MemcachedCompatibleClient;
import com.yahoo.ycsb.memcached.MemcachedCompatibleConfig;
import com.yahoo.ycsb.workloads.RangeScanOperation;
import net.spy.memcached.MemcachedClient;

import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"NullableProblems"})
public class CouchbaseClient2_0 extends MemcachedCompatibleClient implements RangeScanOperation {

    private static final SecureRandom RANDOM = new SecureRandom();

    public static final String SCAN_DESIGN_DOCUMENT_NAME = "";

    public static final String SCAN_VIEW_NAME = "_all_docs";

//    protected View scanView;

    private CouchbaseConfig couchbaseConfig;

    @Override
    protected MemcachedCompatibleConfig createMemcachedConfig() {
        return couchbaseConfig = new CouchbaseConfig(getProperties());
    }

    @Override
    protected MemcachedClient createMemcachedClient() throws Exception {
        return createCouchbaseClient();
    }

    protected CouchbaseClient createCouchbaseClient() throws Exception {
        CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();
        builder.setReadBufferSize(config.getReadBufferSize());
        builder.setOpTimeout(config.getOpTimeout());
        builder.setFailureMode(config.getFailureMode());

        List<URI> servers = new ArrayList<URI>();
        for (String address : config.getHosts().split(",")) {
            servers.add(new URI("http://" + address + ":8091/pools"));
        }
        CouchbaseConnectionFactory connectionFactory =
                builder.buildCouchbaseConnection(servers,
                        couchbaseConfig.getBucket(), couchbaseConfig.getUser(), couchbaseConfig.getPassword());
        return new com.couchbase.client.CouchbaseClient(connectionFactory);
    }

    @Override
    public int scan(String table, String startKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
//        try {
//            Query query = new Query();
//            query.setIncludeDocs(true);
//            query.setRangeStart(createQualifiedKey(table, startKey));
//            query.setLimit(limit);
//            scanQuery(query, result);
//            return OK;
//        } catch (Exception e) {
//            if (log.isErrorEnabled()) {
//                log.error("Error performing scan starting with a key: " + startKey, e);
//            }
//            return ERROR;
//        }
        throw new UnsupportedOperationException("Scan not implemented");
    }

    @Override
    public int scan(String table, String startKey, String endKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
//        try {
//            Query query = new Query();
//            query.setIncludeDocs(true);
//            query.setRangeStart(createQualifiedKey(table, startKey));
//            query.setRangeEnd(createQualifiedKey(table, endKey));
//            query.setLimit(limit);
//            scanQuery(query, result);
//            return OK;
//        } catch (Exception e) {
//            if (log.isErrorEnabled()) {
//                log.error("Error performing scan starting with a key: " + startKey, e);
//            }
//            return ERROR;
//        }
        throw new UnsupportedOperationException("Scan not implemented");
    }

//    protected void scanQuery(Query query, List<Map<String, ByteIterator>> result) throws IOException {
//        ViewResponse response = ((CouchbaseClient) client).query(createScanView(), query);
//        for (ViewRow row : response) {
//            Map<String, ByteIterator> documentAsMap = new HashMap<String, ByteIterator>();
//            fromJson((String) row.getDocument(), null, documentAsMap);
//            result.add(documentAsMap);
//        }
//    }
//
//    protected View createScanView() {
//        if (scanView == null) {
//            scanView = ((CouchbaseClient) client).getView(SCAN_DESIGN_DOCUMENT_NAME, SCAN_VIEW_NAME);
//        }
//        return scanView;
//    }
}
