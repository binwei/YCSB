/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package com.yahoo.ycsb.hbase;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Client uses /hbase-conf/hbase-site.xml configuration to connect to HBase cluster
 */
public class HBaseClient extends DB implements HBaseClientProperties {

    private static final Configuration config = HBaseConfiguration.create();

    static {
        config.addResource("/master/hbase-site.xml");
    }

    public boolean debug = false;

    public String table = "";
    public HTable hTable = null;
    public String columnFamily = "";
    public byte columnFamilyBytes[];

    public static final Object tableLock = new Object();

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        if (Boolean.valueOf(getProperties().getProperty(DEBUG_PROPERTY))) {
            debug = true;
        }
        columnFamily = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
        columnFamilyBytes = Bytes.toBytes(columnFamily);
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
        try {
            if (hTable != null) {
                hTable.flushCommits();
            }
        } catch (IOException e) {
            throw new DBException(e);
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param values a map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> values) {
        try {
            initHTable(table);
        } catch (IOException e) {
            return ERROR;
        }
        Result result;
        try {
            if (debug) {
                System.out.println("Doing read from HBase column family " + columnFamily);
                System.out.println("Doing read for key: " + key);
            }
            Get get = new Get(Bytes.toBytes(key));
            if (fields == null) {
                get.addFamily(columnFamilyBytes);
            } else {
                for (String field : fields) {
                    get.addColumn(columnFamilyBytes, Bytes.toBytes(field));
                }
            }
            result = hTable.get(get);
        } catch (IOException e) {
            System.err.println("Error doing get: " + e);
            return ERROR;
        } catch (ConcurrentModificationException e) {
            //do nothing for now...need to understand HBase concurrency model better
            return ERROR;
        }

        for (KeyValue kv : result.raw()) {
            values.put(
                    Bytes.toString(kv.getQualifier()),
                    new ByteArrayByteIterator(kv.getValue()));
            if (debug) {
                System.out.println("Result for field: " + Bytes.toString(kv.getQualifier()) +
                        " is: " + Bytes.toString(kv.getValue()));
            }

        }
        return OK;
    }

    private void initHTable(String table) throws IOException {
        if (!this.table.equals(table)) {
            synchronized (tableLock) {
                HTable hTable = new HTable(config, table);
                // 2 suggestions from http://ryantwopointoh.blogspot.com/2009/01/performance-of-hbase-importing.html
                hTable.setAutoFlush(false);
                hTable.setWriteBufferSize(1024 * 1024 * 12);
                this.hTable = hTable;
                this.table = table;
            }
        }
    }


    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startKey    The record key of the first record to read.
     * @param records The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startKey, int records, Set<String> fields, List<Map<String, ByteIterator>> result) {
        try {
            initHTable(table);
        } catch (IOException e) {
            return ERROR;
        }
        Scan scan = new Scan(Bytes.toBytes(startKey));
        //HBase has no record limit.  Here, assume recordCount is small enough to bring back in one call.
        //We get back recordCount records
        scan.setCaching(records);

        //add specified fields or else all fields
        if (fields == null) {
            scan.addFamily(columnFamilyBytes);
        } else {
            for (String field : fields) {
                scan.addColumn(columnFamilyBytes, Bytes.toBytes(field));
            }
        }

        //get results
        ResultScanner scanner = null;
        try {
            scanner = hTable.getScanner(scan);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                //get row key
                String key = Bytes.toString(rr.getRow());
                if (debug) {
                    System.out.println("Got scan result for key: " + key);
                }

                Map<String, ByteIterator> rowResult = new HashMap<String, ByteIterator>();

                for (KeyValue kv : rr.raw()) {
                    rowResult.put(
                            Bytes.toString(kv.getQualifier()),
                            new ByteArrayByteIterator(kv.getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult);
                numResults++;
                if (numResults >= records) //if hit recordCount, bail out
                {
                    break;
                }
            } //done with row

        } catch (IOException e) {
            if (debug) {
                System.out.println("Error processing scan result: " + e);
            }
            return ERROR;
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return OK;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, Map<String, ByteIterator> values) {
        //if this is a "new" table, init HTable object.  Else, use existing one
        try {
            initHTable(table);
        } catch (IOException e) {
            System.out.print("Exception " + e);
            return ERROR;
        }
        if (debug) {
            System.out.println("Setting up put for key: " + key);
        }
        Put put = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            if (debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/" +
                        entry.getValue() + " to put request");
            }
            put.add(columnFamilyBytes, Bytes.toBytes(entry.getKey()), entry.getValue().toArray());
        }
        try {
            hTable.put(put);
        } catch (IOException e) {
            if (debug) {
                System.err.println("Error doing put: " + e);
            }
            return ERROR;
        } catch (ConcurrentModificationException e) {
            //do nothing for now...hope this is rare
            return ERROR;
        }
        return OK;
    }


    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        return update(table, key, values);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key) {
        //if this is a "new" table, init HTable object.  Else, use existing one
        try {
            initHTable(table);
        } catch (IOException e) {
            return ERROR;
        }
        if (debug) {
            System.out.println("Doing delete for key: " + key);
        }
        Delete delete = new Delete(Bytes.toBytes(key));
        try {
            hTable.delete(delete);
        } catch (IOException e) {
            if (debug) {
                System.err.println("Error doing delete: " + e);
            }
            return ERROR;
        }
        return OK;
    }
}
