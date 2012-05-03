package com.yahoo.ycsb.couchbase;

public interface CouchbaseClientProperties {
    
    final String BUCKET_PROPERTY = "couchbase.bucket";

    final String BUCKET_PROPERTY_DEFAULT = "default";

    final String USER_PROPERTY = "couchbase.user";

    final String PASSWORD_PROPERTY = "couchbase.password";

    final String HOSTS_PROPERTY = "couchbase.hosts";

    final int DEFAULT_TIMEOUT = 60000;

    final String TIMEOUT_PROPERTY = "couchbase.timeout";

    final long SHUTDOWN_TIMEOUT_MILLIS = 3000;
}
