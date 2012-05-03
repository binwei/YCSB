package com.yahoo.ycsb.riak;

import com.basho.riak.client.raw.Transport;

public interface RiakClientProperties {

    final String HOSTS_PROPERTY = "hosts";

    final String TRANSPORT_PROPERTY = "riak.transport";

    final String DATA_FIELD_PROPERTY = "riak.datafield";

    final String PB_IDLE_CONNECTION_TTL_MILLIS = "riak.pb.idleConnectionTTLMillis";

    final int PB_IDLE_CONNECTION_TTL_MILLIS_DEFAULT = 3000;

    final String PB_CONNECTION_TIMEOUT_MILLIS = "riak.pb.connectionTimeoutMillis";

    final int PB_CONNECTION_TIMEOUT_MILLIS_DEFAULT = 3000;

    final String HTTP_TIMEOUT = "riak.http.timeout";

    final int HTTP_TIMEOUT_DEFAULT = 3000;

    final Transport TRANSPORT_PROPERTY_DEFAULT = Transport.PB;

    final RiakDataField DATA_FIELD_DEFAULT = RiakDataField.META;
}
