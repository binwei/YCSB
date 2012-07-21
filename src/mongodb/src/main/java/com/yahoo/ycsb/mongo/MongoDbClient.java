/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */
package com.yahoo.ycsb.mongo;

import com.mongodb.*;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mongodb.ReadPreference.PRIMARY;
import static com.mongodb.WriteConcern.NORMAL;
import static com.yahoo.ycsb.StringByteIterator.getByteIteratorMap;

/**
 * MongoDB client for YCSB framework.
 * <p/>
 * Properties to set:
 * <p/>
 * mongodb.urls=localhost:27017 (comma separated list)
 * mongodb.database=UserDatabase
 * mongodb.writeConcern=normal
 *
 * @author ypai
 */
public class MongoDbClient extends DB implements MongoDbClientProperties {

    private static Random random = new Random();
    protected final Logger log = LoggerFactory.getLogger(getClass());
    private Mongo mongo;
    private String database;
    private ReadPreference readPreference;
    private WriteConcern writeConcern;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        Properties properties = getProperties();
        database = properties.getProperty(DATABASE);
        String[] urls = properties.getProperty(URLS).split(",");
        String url = urls[random.nextInt(urls.length)];
        readPreference = getReadPreference();
        writeConcern = getWriteConcern();
        try {
            /**
             * Strip out prefix since Java driver doesn't currently support standard connection format URL yet
             * http://www.mongodb.org/display/DOCS/Connections
             */
            if (url.startsWith("mongodb://")) {
                url = url.substring(10);
            }
            mongo = new Mongo(new DBAddress(url, database));
            mongo.setReadPreference(readPreference);
            mongo.setWriteConcern(writeConcern);
        } catch (Exception exception) {
            if (log.isErrorEnabled()) {
                log.error("Could not initialize connection", exception);
            }
        }
    }

    private ReadPreference getReadPreference() {
        String value = getProperties().getProperty(READ_PREFERENCE);
        return value != null ? ReadPreferenceHelper.valueOf(value.toUpperCase()) : PRIMARY;
    }

    private WriteConcern getWriteConcern() {
        String value = getProperties().getProperty(WRITE_CONCERN);
        return value != null ? WriteConcern.valueOf(value.toUpperCase()) : NORMAL;
    }

    @Override
    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject query = new BasicDBObject("_id", key);
            if (WriteConcern.SAFE.equals(writeConcern)) {
                query.put("$atomic", true);
            }
            collection.remove(query);
            return db.getLastError().ok() ? OK : ERROR;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error deleting value", e);
            }
            return ERROR;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    public int deleteAll(String table, Set<String> keys) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            BasicDBList keysIn = new BasicDBList();
            keysIn.addAll(keys);
            DBObject query = new BasicDBObject("_id", new BasicDBObject("$in", keysIn));
            if (WriteConcern.SAFE.equals(writeConcern)) {
                query.put("$atomic", true);
            }
            collection.remove(query);
            return db.getLastError().ok() ? OK : ERROR;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error deleting value", e);
            }
            return ERROR;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    @Override
    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject object = new BasicDBObject("_id", key);
            for (String k : values.keySet()) {
                object.put(k, values.get(k).toString());
            }
            collection.insert(object);
            return db.getLastError().ok() ? OK : ERROR;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error inserting value", e);
            }
            return ERROR;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields,
                    Map<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject query = new BasicDBObject("_id", key);
            DBObject targetFields = null;
            if (fields != null) {
                targetFields = new BasicDBObject();
                for (String field : fields) {
                    targetFields.put(field, 1);
                }
            }
            DBObject object = collection.findOne(query, targetFields);
            if (object != null) {
                result.putAll(getByteIteratorMap(object.toMap()));
            }
            return object != null ? OK : ERROR;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error inserting value", e);
            }
            return ERROR;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }


    @Override
    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, Map<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject targetFields = new BasicDBObject();
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                targetFields.put(entry.getKey(), entry.getValue().toString());
            }
            collection.setWriteConcern(writeConcern);
            collection.update(new BasicDBObject("_id", key), new BasicDBObject("$set", targetFields));
            return db.getLastError().ok() ? OK : ERROR;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error inserting value", e);
            }
            return ERROR;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startKey, int limit,
                    Set<String> fields, List<Map<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);

            DBObject query = new BasicDBObject("_id", new BasicDBObject("$gte", startKey));
            DBCursor object = collection.find(query).limit(limit);
            while (object.hasNext()) {
                //toMap() returns a Map, but result.add() expects a Map<String,String>. Hence, the suppress warnings.
                result.add(getByteIteratorMap((Map<String, String>) object.next().toMap()));
            }
            return OK;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error performing range scan", e);
            }
            return ERROR;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }
}