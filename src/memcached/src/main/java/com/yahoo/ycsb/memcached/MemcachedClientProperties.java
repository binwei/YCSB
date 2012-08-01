package com.yahoo.ycsb.memcached;

import net.spy.memcached.FailureMode;

public interface MemcachedClientProperties {

    final String ADDRESSES_PROPERTY = "memcached.addresses";

    final int DEFAULT_PORT = 11211;

    final long SHUTDOWN_TIMEOUT_MILLIS = 30000;

    final int OBJECT_EXPIRATION_TIME = Integer.MAX_VALUE;

    final String CHECK_OPERATION_STATUS_PROPERTY = "memcached.checkOperationStatus";

    final boolean CHECK_OPERATION_STATUS_DEFAULT = false;

    final int DEFAULT_TIMEOUT = 60000;

    final String TIMEOUT_PROPERTY = "memcached.timeout";

    final String READ_BUFFER_SIZE_PROPERTY = "memcached.readBufferSize";

    final int READ_BUFFER_SIZE_DEFAULT = 1024 * 1024 * 2;

    final String FAILURE_MODE_PROPERTY = "memcached.failureMode";

    final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;

}
