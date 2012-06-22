package com.yahoo.ycsb.jdbc;

import java.sql.Connection;

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
        return new ShardedQueryDescriptor(type, table, getShard(key), parameters);
    }
}
