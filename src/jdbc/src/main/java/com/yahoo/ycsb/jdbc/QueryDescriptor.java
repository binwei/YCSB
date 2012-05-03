package com.yahoo.ycsb.jdbc;

public interface QueryDescriptor {

    QueryType getType();

    String getTable();

    int getParameters();
}
