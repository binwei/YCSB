package com.yahoo.ycsb.mongo;

public interface MongoDbClientProperties {

    final String URLS = "mongodb.urls";

    final String DATABASE = "mongodb.database";

    final String WRITE_CONCERN = "mongodb.writeConcern";

    final String READ_PREFERENCE = "mongodb.readPreference";
}
