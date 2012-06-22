/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */
package com.yahoo.ycsb.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static com.yahoo.ycsb.StringByteIterator.getByteIteratorMap;

/**
 * MongoDB client for YCSB framework.
 * <p/>
 * Properties to set:
 * <p/>
 * mongodb.urls=mongodb://localhost:27017 (comma separated list of mongo routers
 * mongodb.database=UserDatabase
 * mongodb.writeConcern=normal
 *
 * @author ypai
 */
public class MongoDbClient extends DB {

    private static Random random = new Random();
    private Mongo mongo;
    private WriteConcern writeConcern;
    private String database;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize MongoDb driver
        Properties props = getProperties();
        String urls = props.getProperty("mongodb.urls");
        database = props.getProperty("mongodb.database");
        String writeConcernType = props.getProperty("mongodb.writeConcern");

        if ("none".equals(writeConcernType)) {
            writeConcern = WriteConcern.NONE;
        } else if ("strict".equals(writeConcernType)) {
            writeConcern = WriteConcern.SAFE;
        } else if ("normal".equals(writeConcernType)) {
            writeConcern = WriteConcern.NORMAL;
        }
        String []allUrls = urls.split(",");
        String url = allUrls[random.nextInt(allUrls.length)];
        try {
            // strip out prefix since Java driver doesn't currently support
            // standard connection format URL yet
            // http://www.mongodb.org/display/DOCS/Connections
            if (url.startsWith("mongodb://")) {
                url = url.substring(10);
            }
            // need to append db to url.
            url += "/" + database;
            mongo = new Mongo(new DBAddress(url));
        } catch (Exception exception) {
            System.err.println("Could not initialize mongo connection: " + exception.toString());
            return;
        }
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
            DBObject q = new BasicDBObject().append("_id", key);
            if (writeConcern.equals(WriteConcern.SAFE)) {
                q.put("$atomic", true);
            }
            collection.remove(q);

            // see if record was deleted
            DBObject errors = db.getLastError();

            return ((Integer) errors.get("n")) == 1 ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
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
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toString());
            }
            collection.setWriteConcern(writeConcern);

            collection.insert(r);

            // determine if record was inserted, does not seem to return
            // n=<records affected> for insert
            DBObject errors = db.getLastError();

            return ((Double) errors.get("ok") == 1.0) && errors.get("err") == null ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
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
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();
            boolean returnAllFields = fields == null;
            DBObject queryResult;
            if (!returnAllFields) {
                for (String field : fields) {
                    fieldsToReturn.put(field, 1);
                }
                queryResult = collection.findOne(q, fieldsToReturn);
            } else {
                queryResult = collection.findOne(q);
            }
            if (queryResult != null) {
                result.putAll(getByteIteratorMap(queryResult.toMap()));
            }
            return queryResult != null ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
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
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            String tmpKey, tmpVal;
            while (keys.hasNext()) {
                tmpKey = keys.next();
                tmpVal = values.get(tmpKey).toString();
                fieldsToSet.put(tmpKey, tmpVal);

            }
            u.put("$set", fieldsToSet);
            collection.setWriteConcern(writeConcern);
            collection.update(q, u);

            // determine if record was inserted
            DBObject errors = db.getLastError();

            return (Integer) errors.get("n") == 1 ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
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
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startKey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBCursor cursor = collection.find(q).limit(limit);
            while (cursor.hasNext()) {
                //toMap() returns a Map, but result.add() expects a Map<String,String>. Hence, the suppress warnings.
                result.add(getByteIteratorMap((Map<String, String>) cursor.next().toMap()));
            }
            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }
}