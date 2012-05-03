package com.yahoo.ycsb.riak;

public class RiakClientException extends RuntimeException {

    public RiakClientException(String message) {
        super(message);
    }

    public RiakClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public RiakClientException(Throwable cause) {
        super(cause);
    }
}
