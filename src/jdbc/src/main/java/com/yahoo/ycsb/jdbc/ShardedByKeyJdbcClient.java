package com.yahoo.ycsb.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.yahoo.ycsb.jdbc.QueryType.*;
import static java.util.Arrays.asList;

public class ShardedByKeyJdbcClient extends BaseJdbcClient {

    /**
     * For the given key, returns what shard contains data for this key
     *
     * @param key Data key to do operation on
     * @return Shard index
     */
    protected int getShard(String key) {
        return Math.abs(key.hashCode()) % connections.size();
    }

    /**
     * For the given key, returns Connection object that holds connection
     * to the shard that contains this key
     *
     * @return Connection object
     */
    @Override
    protected Connection getConnection(QueryDescriptor descriptor) {
        int shard = ((ShardedQueryDescriptor) descriptor).getShard();
        return connections.get(shard);
    }

    @Override
    protected QueryDescriptor createQueryDescriptor(QueryType type, String table, String key, int parameters) {
        return createQueryDescriptor(type, table, getShard(key), parameters);
    }

    @Override
    protected QueryDescriptor[] createQueryDescriptors() {
        List<QueryDescriptor> descriptors = new ArrayList<QueryDescriptor>();
        int shards = connections.size();
        for (int shard = 0; shard < shards; shard++) {
            descriptors.addAll(asList(
                    createQueryDescriptor(READ, table, shard, 1),
                    createQueryDescriptor(SCAN, table, shard, 1),
                    createQueryDescriptor(INSERT, table, shard, fieldCount),
                    createQueryDescriptor(UPDATE, table, shard, fieldCount),
                    createQueryDescriptor(DELETE, table, shard, 1)));
        }
        return descriptors.toArray(new QueryDescriptor[descriptors.size()]);
    }

    protected QueryDescriptor createQueryDescriptor(QueryType type, String table, int shard, int parameters) {
        return new ShardedQueryDescriptor(type, table, shard, parameters);
    }
}
