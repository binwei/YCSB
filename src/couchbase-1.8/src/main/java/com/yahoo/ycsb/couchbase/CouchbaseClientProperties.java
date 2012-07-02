package com.yahoo.ycsb.couchbase;

import net.spy.memcached.FailureMode;

public interface CouchbaseClientProperties {
    
    final String BUCKET_PROPERTY = "couchbase.bucket";

    final String BUCKET_PROPERTY_DEFAULT = "default";

    final String USER_PROPERTY = "couchbase.user";

    final String PASSWORD_PROPERTY = "couchbase.password";

    final String HOSTS_PROPERTY = "couchbase.hosts";

    final int DEFAULT_TIMEOUT = 60000;

    final String TIMEOUT_PROPERTY = "couchbase.timeout";

    final long SHUTDOWN_TIMEOUT_MILLIS = 30000;

    final int OBJECT_EXPIRATION_TIME = Integer.MAX_VALUE;

    final String READ_BUFFER_SIZE_PROPERTY = "couchbase.readBufferSize";

    final int READ_BUFFER_SIZE_DEFAULT = 1024 * 1024 * 2;

    final String FAILURE_MODE_PROPERTY = "couchbase.failureMode";

    final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;

    final String CHECK_OPERATION_STATUS_PROPERTY = "couchbase.checkOperationStatus";

    final boolean CHECK_OPERATION_STATUS_DEFAULT = false;
}
