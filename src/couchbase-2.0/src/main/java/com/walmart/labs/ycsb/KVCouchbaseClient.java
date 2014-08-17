package com.walmart.labs.ycsb;

import com.walmart.labs.search.api.compress.GZipTextCompressor;
import com.walmart.labs.search.cache.caas.CaasClient;
import com.walmart.labs.search.cache.config.CAASConfig;
import com.walmart.labs.search.cache.core.CacheSegments;
import com.walmart.labs.search.cache.core.CacheUtil;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by binwei on 8/17/14.
 */
public class KVCouchbaseClient extends DB {
    private static final GZipTextCompressor compressor = new GZipTextCompressor();

    private CaasClient client;
    private long noCacheHit = 0;
    private long noCacheMiss = 0;
    private long noSetPassed = 0;
    private long noSetFailed = 0;

    @Override
    public void init() throws DBException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(CAASConfig.CAASConfigKey.LOCAL_PROPERTY_FILE.name(), getProperties().getProperty("kvstore.localConfigLocation"));

        CAASConfig caasConfig = new CAASConfig(properties, CacheSegments.kvstore.name());
        client = new CaasClient(caasConfig);
    }

    @Override
    public void cleanup() throws DBException {
        System.out.println("INSERT - Number of cache set failed :" + noSetFailed);
        System.out.println("INSERT - Number of cache set passed :" + noSetPassed);
        double successRate = (noSetPassed * 1.0 / (noSetFailed + noSetPassed)) * 100;
        System.out.println("INSERT - Success percentage:" + successRate);

        System.out.println("READ - Number of cache misses :" + noCacheMiss);
        System.out.println("READ - Number of cache hits :" + noCacheHit);
        successRate = (noCacheHit * 1.0 / (noCacheHit + noCacheMiss)) * 100;
        System.out.println("READ - Cache hit ratio :" + successRate);

        super.cleanup();
    }

    @Override
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        if (client.isDisabled()) {
            return ERROR;
        }

        String rawCacheKey = CacheUtil.getKVStoreCacheKey(key, "");
        byte[] bytes = client.get(rawCacheKey, CacheUtil.DEFAULT_CACHE_VERSION, true);

        if (null != bytes) {
            noCacheHit++;
            String fieldValue = compressor.uncompress(bytes);
            result.put(key, new StringByteIterator(fieldValue));
        } else {
            noCacheMiss++;
        }
        return OK;
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        if (client.isDisabled()) {
            return ERROR;
        }

        String rawCacheKey = CacheUtil.getKVStoreCacheKey(key, "");
        Map<String, byte[]> keyValues = new HashMap<String, byte[]>();
        HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
        final String fieldValue = stringMap.values().iterator().next();
        keyValues.put(rawCacheKey, compressor.compress(fieldValue));

        if (client.set(keyValues, CacheUtil.DEFAULT_CACHE_VERSION)) {
            noSetPassed++;
            return OK;
        } else {
            noSetFailed++;
            return ERROR;
        }
    }

    @Override
    public int scan(String table, String startKey, int limit, Set<String> fields, List<Map<String, ByteIterator>> result) {
        throw new UnsupportedOperationException("Scan not implemented");
    }

    @Override
    public int update(String table, String key, Map<String, ByteIterator> values) {
        throw new UnsupportedOperationException("Update not implemented");
    }

    @Override
    public int delete(String table, String key) {
        throw new UnsupportedOperationException("Delete not implemented");
    }
}
