package com.yahoo.ycsb.memcached;

import net.spy.memcached.ConnectionFactoryBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MemcachedClient extends MemcachedClientBase {

    @Override
    protected net.spy.memcached.MemcachedClient createMemcachedClient() throws Exception {
        ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();
        initConnectionFactoryBuilder(connectionFactoryBuilder);
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        String addressesValue = getProperties().getProperty(ADDRESSES_PROPERTY);
        for (String address : addressesValue.split(",")) {
            int colon = address.indexOf(":");
            int port = DEFAULT_PORT;
            String host = address;
            if (colon != -1) {
                port = Integer.parseInt(address.substring(colon + 1));
                host = address.substring(0, colon);
            }
            addresses.add(new InetSocketAddress(host, port));
        }
        return new net.spy.memcached.MemcachedClient(connectionFactoryBuilder.build(), addresses);
    }
}
