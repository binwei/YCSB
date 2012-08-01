package com.yahoo.ycsb.couchbase;

import com.yahoo.ycsb.memcached.MemcachedClientProperties;

public interface CouchbaseClientProperties extends MemcachedClientProperties {

    final String BUCKET_PROPERTY = "couchbase.bucket";

    final String BUCKET_PROPERTY_DEFAULT = "default";

    final String USER_PROPERTY = "couchbase.user";

    final String PASSWORD_PROPERTY = "couchbase.password";

    final String HOSTS_PROPERTY = "couchbase.hosts";
}
