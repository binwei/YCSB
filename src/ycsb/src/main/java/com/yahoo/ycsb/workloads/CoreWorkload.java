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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.yahoo.ycsb.Utils.*;
import static com.yahoo.ycsb.generator.ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT;
import static com.yahoo.ycsb.generator.ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * <p/>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldCount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class CoreWorkload extends Workload {

    public static final int CLEANUP_INSERTED_KEYS_BATCH_SIZE = 1000;
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * The name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY = "table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final int FIELD_COUNT_PROPERTY_DEFAULT = 10;

    /**
     * The name of the property for the field length distribution. Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
     * <p/>
     * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
     * histogram will be read from the filename specified in the "fieldlengthhistogram" property.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
    /**
     * The default field length distribution.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
    /**
     * The default maximum length of a field in bytes.
     */
    public static final int FIELD_LENGTH_PROPERTY_DEFAULT = 100;

    /**
     * The name of a property that specifies the filename containing the field length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
    /**
     * The default filename containing a field length histogram.
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    /**
     * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
     */
    public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final boolean READ_ALL_FIELDS_PROPERTY_DEFAULT = true;

    /**
     * The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

    /**
     * The default value for the writeallfields property.
     */
    public static final boolean WRITE_ALL_FIELDS_PROPERTY_DEFAULT = false;

    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final double READ_PROPORTION_PROPERTY_DEFAULT = 0.95;

    /**
     * The name of the property for the proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final double UPDATE_PROPORTION_PROPERTY_DEFAULT = 0.05;

    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final double INSERT_PROPORTION_PROPERTY_DEFAULT = 0.0;

    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final double SCAN_PROPORTION_PROPERTY_DEFAULT = 0.0;

    /**
     * The name of the property for the proportion of transactions that are read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final double READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = 0.0;

    /**
     * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the max scan length (number of records)
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

    /**
     * The default max scan length.
     */
    public static final int MAX_SCAN_LENGTH_PROPERTY_DEFAULT = 1000;

    /**
     * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the order to insert records. Options are "ordered" or "hashed"
     */
    public static final String INSERT_ORDER_PROPERTY = "insertorder";

    /**
     * Default insert order.
     */
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final double HOTSPOT_DATA_FRACTION_DEFAULT = 0.2;

    /**
     * Percentage operations that access the hot set.
     */
    public static final String HOTSPOT_OPERATION_FRACTION = "hotspotoperationfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final double HOTSPOT_OPERATION_FRACTION_DEFAULT = 0.8;

    public static final String DISTRIBUTION_EXPONENTIAL = "exponential";

    public static final String DISTRIBUTION_UNIFORM = "uniform";

    public static final String DISTRIBUION_ZIPFIAN = "zipfian";

    public static final String DISTRIBUTION_LATEST = "latest";

    public static final String DISTRIBUTION_HOTSPOT = "hotspot";

    public static final String DISTRIBUTION_CONSTANT = "constant";

    public static final String DISTRIBUTION_HISTOGRAM = "histogram";

    /**
     * Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
     */
    protected IntegerGenerator fieldLengthGenerator;

    protected boolean readAllFields;

    protected static String table;

    protected int fieldCount;

    protected boolean writeAllFields;

    protected IntegerGenerator insertKeyGenerator;

    protected Generator operationChooser;

    protected IntegerGenerator transactionKeyGenerator;

    protected Generator fieldChooser;

    protected IntegerGenerator transactionInsertKeyGenerator;

    protected IntegerGenerator scanLengthGenerator;

    protected boolean orderedInserts;

    protected int recordCount;

    protected Properties properties;

    private boolean cleanupInsertedKeys;

    private List<String> insertedKeys;

    public static final String CLEANUP_INSERTED_KEYS_PROPERTY = "cleanupinsertedkeys";

    private int operationCount;

    private double insertProportion;
    private double readProportion;
    private double updateProportion;
    private double scanProportion;
    private double readModifyWriteProportion;

    public static IntegerGenerator createFieldLengthGenerator(Properties properties) throws WorkloadException {
        IntegerGenerator generator;
        String distribution = properties.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int length = parseInt(properties.getProperty(FIELD_LENGTH_PROPERTY), FIELD_LENGTH_PROPERTY_DEFAULT);
        if (distribution.compareTo(DISTRIBUTION_CONSTANT) == 0) {
            generator = new ConstantIntegerGenerator(length);
        } else if (distribution.compareTo(DISTRIBUTION_UNIFORM) == 0) {
            generator = new UniformIntegerGenerator(1, length);
        } else if (distribution.compareTo(DISTRIBUION_ZIPFIAN) == 0) {
            generator = new ZipfianGenerator(1, length);
        } else if (distribution.compareTo(DISTRIBUTION_HISTOGRAM) == 0) {
            String histogram = properties.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
            try {
                generator = new HistogramGenerator(histogram);
            } catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + histogram, e);
            }
        } else {
            throw new WorkloadException("Unknown field length distribution " + distribution);
        }
        return generator;
    }

    public void init(Properties properties) throws WorkloadException {
        this.properties = properties;
        init();
    }

    /**
     * Initialize the scenario. Called once, in the main client thread, before any operations are started.
     *
     * @throws com.yahoo.ycsb.WorkloadException
     *          if workload initialization failed.
     */
    protected void init() throws WorkloadException {
        operationCount = Integer.parseInt(
                properties.getProperty(Client.OPERATION_COUNT_PROPERTY));
        readProportion = parseDouble(
                properties.getProperty(READ_PROPORTION_PROPERTY), READ_PROPORTION_PROPERTY_DEFAULT);
        updateProportion = parseDouble(
                properties.getProperty(UPDATE_PROPORTION_PROPERTY), UPDATE_PROPORTION_PROPERTY_DEFAULT);
        insertProportion = parseDouble(
                properties.getProperty(INSERT_PROPORTION_PROPERTY), INSERT_PROPORTION_PROPERTY_DEFAULT);
        scanProportion = parseDouble(
                properties.getProperty(SCAN_PROPORTION_PROPERTY), SCAN_PROPORTION_PROPERTY_DEFAULT);
        readModifyWriteProportion = parseDouble(
                properties.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY), READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT);

        table = properties.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

        fieldCount = parseInt(properties.getProperty(FIELD_COUNT_PROPERTY), FIELD_COUNT_PROPERTY_DEFAULT);
        fieldLengthGenerator = CoreWorkload.createFieldLengthGenerator(properties);
        recordCount = Integer.parseInt(properties.getProperty(Client.RECORD_COUNT_PROPERTY));

        readAllFields = parseBoolean(properties.getProperty(READ_ALL_FIELDS_PROPERTY), READ_ALL_FIELDS_PROPERTY_DEFAULT);
        writeAllFields = parseBoolean(properties.getProperty(WRITE_ALL_FIELDS_PROPERTY), WRITE_ALL_FIELDS_PROPERTY_DEFAULT);

        orderedInserts = properties.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") != 0;

        fieldChooser = createFieldChooser();
        operationChooser = createOperationChooser();
        insertKeyGenerator = createKeyGenerator();
        transactionInsertKeyGenerator = createTransactionInsertKeyGenerator();
        transactionKeyGenerator = createTransactionKeyGenerator();
        scanLengthGenerator = createScanLengthGenerator();

        cleanupInsertedKeys = Utils.parseBoolean(properties.getProperty(CLEANUP_INSERTED_KEYS_PROPERTY), false);
        if (cleanupInsertedKeys) {
            int insertedKeysCapacity = (int) (insertProportion * operationCount * 2.0);
            insertedKeys = new ArrayList<String>(insertedKeysCapacity);
        }
    }

    protected IntegerGenerator createTransactionInsertKeyGenerator() {
        return new CounterGenerator(recordCount);
    }

    protected Generator createFieldChooser() {
        return new UniformIntegerGenerator(0, fieldCount - 1);
    }

    protected IntegerGenerator createTransactionKeyGenerator() throws WorkloadException {
        IntegerGenerator generator;
        String distribution = properties.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        if (distribution.compareTo(DISTRIBUTION_EXPONENTIAL) == 0) {
            double percentile = parseDouble(properties.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY),
                    EXPONENTIAL_PERCENTILE_DEFAULT);
            double frac = parseDouble(properties.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY),
                    EXPONENTIAL_FRAC_DEFAULT);
            generator = new ExponentialGenerator(percentile, recordCount * frac);
        } else if (distribution.compareTo(DISTRIBUTION_UNIFORM) == 0) {
            generator = new UniformIntegerGenerator(0, recordCount - 1);
        } else if (distribution.compareTo(DISTRIBUION_ZIPFIAN) == 0) {
            //it does this by generating a random "next key" in part by taking the modulus over the number of keys
            //if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
            //so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
            //of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            //plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
            //just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
            int expectedNewKeys = (int) (insertProportion * operationCount * 2.0); //2 is fudge factor
            generator = new ScrambledZipfianGenerator(recordCount + expectedNewKeys);
        } else if (distribution.compareTo(DISTRIBUTION_LATEST) == 0) {
            generator = new SkewedLatestGenerator(transactionInsertKeyGenerator);
        } else if (distribution.equals(DISTRIBUTION_HOTSPOT)) {
            double dataFraction = parseDouble(
                    properties.getProperty(HOTSPOT_DATA_FRACTION), HOTSPOT_DATA_FRACTION_DEFAULT);
            double operationFraction = parseDouble(
                    properties.getProperty(HOTSPOT_OPERATION_FRACTION), HOTSPOT_OPERATION_FRACTION_DEFAULT);
            generator = new HotspotIntegerGenerator(0, recordCount - 1,
                    dataFraction, operationFraction);
        } else {
            throw new WorkloadException("Unknown request distribution " + distribution);
        }
        return generator;
    }

    protected Generator createOperationChooser() {
        DiscreteGenerator chooser = new DiscreteGenerator();
        if (readProportion > 0) {
            chooser.addValue(readProportion, "READ");
        }
        if (updateProportion > 0) {
            chooser.addValue(updateProportion, "UPDATE");
        }
        if (insertProportion > 0) {
            chooser.addValue(insertProportion, "INSERT");
        }
        if (scanProportion > 0) {
            chooser.addValue(scanProportion, "SCAN");
        }
        if (readModifyWriteProportion > 0) {
            chooser.addValue(readModifyWriteProportion, "READMODIFYWRITE");
        }
        return chooser;
    }

    protected IntegerGenerator createKeyGenerator() {
        return new CounterGenerator(parseInt(properties.getProperty(INSERT_START_PROPERTY), INSERT_START_PROPERTY_DEFAULT));
    }

    protected IntegerGenerator createScanLengthGenerator() throws WorkloadException {
        IntegerGenerator generator;
        String distribution = properties.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int length = parseInt(properties.getProperty(MAX_SCAN_LENGTH_PROPERTY), MAX_SCAN_LENGTH_PROPERTY_DEFAULT);
        if (distribution.compareTo(DISTRIBUTION_UNIFORM) == 0) {
            generator = new UniformIntegerGenerator(1, length);
        } else if (distribution.compareTo(DISTRIBUION_ZIPFIAN) == 0) {
            generator = new ZipfianGenerator(1, length);
        } else {
            throw new WorkloadException("Distribution " + distribution + " not allowed for scan length");
        }
        return generator;
    }

    public String buildKey(long key) {
        if (!orderedInserts) {
            key = Utils.hash(key);
        }
        return "user" + key;
    }

    protected Map<String, ByteIterator> buildValues() {
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for (int i = 0; i < fieldCount; i++) {
            String fieldKey = "field" + i;
            ByteIterator data = new RandomByteIterator(fieldLengthGenerator.nextInt());
            values.put(fieldKey, data);
        }
        return values;
    }

    protected Map<String, ByteIterator> buildUpdate() {
        //update a random field
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String field = "field" + fieldChooser.nextString();
        ByteIterator data = new RandomByteIterator(fieldLengthGenerator.nextInt());
        values.put(field, data);
        return values;
    }

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doInsert(DB db, Object threadState) {
        String key = buildKey(insertKeyGenerator.nextInt());
        Map<String, ByteIterator> values = buildValues();
        return db.insert(table, key, values) == 0;
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doTransaction(DB db, Object threadstate) {
        String op = operationChooser.nextString();
        if (op.compareTo("READ") == 0) {
            doTransactionRead(db);
        } else if (op.compareTo("UPDATE") == 0) {
            doTransactionUpdate(db);
        } else if (op.compareTo("INSERT") == 0) {
            doTransactionInsert(db);
        } else if (op.compareTo("SCAN") == 0) {
            doTransactionScan(db);
        } else {
            doTransactionReadModifyWrite(db);
        }
        return true;
    }

    protected int nextTransactionKey() {
        int key;
        if (transactionKeyGenerator instanceof ExponentialGenerator) {
            do {
                key = transactionInsertKeyGenerator.lastInt() - transactionKeyGenerator.nextInt();
            }
            while (key < 0);
        } else {
            do {
                key = transactionKeyGenerator.nextInt();
            }
            while (key > transactionInsertKeyGenerator.lastInt());
        }
        return key;
    }

    public void doTransactionRead(DB db) {
        //choose a random key
        String key = buildKey(nextTransactionKey());
        Set<String> fields = null;
        if (!readAllFields) {
            //read a random field
            String field = "field" + fieldChooser.nextString();
            fields = new HashSet<String>();
            fields.add(field);
        }
        db.read(table, key, fields, new HashMap<String, ByteIterator>());
    }

    public void doTransactionReadModifyWrite(DB db) {
        //choose a random key
        String key = buildKey(nextTransactionKey());

        Set<String> fields = null;
        if (!readAllFields) {
            // read a random field
            String field = "field" + fieldChooser.nextString();
            fields = new HashSet<String>();
            fields.add(field);
        }

        Map<String, ByteIterator> values;

        if (writeAllFields) {
            //new data for all the fields
            values = buildValues();
        } else {
            //update a random field
            values = buildUpdate();
        }

        // do the transaction

        long st = System.nanoTime();
        db.read(table, key, fields, new HashMap<String, ByteIterator>());
        db.update(table, key, values);
        long en = System.nanoTime();

        Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    }

    public void doTransactionScan(DB db) {
        String startKey = buildKey(nextTransactionKey());
        int length = scanLengthGenerator.nextInt();
        Set<String> fields = null;
        if (!readAllFields) {
            // read a random field
            String field = "field" + fieldChooser.nextString();
            fields = new HashSet<String>();
            fields.add(field);
        }
        db.scan(table, startKey, length, fields, new Vector<Map<String, ByteIterator>>());
    }

    public void doTransactionUpdate(DB db) {
        //choose a random key
        String key = buildKey(nextTransactionKey());
        Map<String, ByteIterator> values;
        if (writeAllFields) {
            //new data for all the fields
            values = buildValues();
        } else {
            //update a random field
            values = buildUpdate();
        }
        db.update(table, key, values);
    }

    public void doTransactionInsert(DB db) {
        //choose the next key
        String key = buildKey(transactionInsertKeyGenerator.nextInt());
        Map<String, ByteIterator> values = buildValues();
        db.insert(table, key, values);
        if (cleanupInsertedKeys) {
            insertedKeys.add(key);
        }
    }

    @Override
    public void cleanup() throws WorkloadException {
        if (cleanupInsertedKeys) {
            if (log.isInfoEnabled()) {
                log.info("Cleaning up inserted keys");
            }
            DB db;
            try {
                db = DBFactory.newDB(properties.getProperty("db", "com.yahoo.ycsb.BasicDB"), properties);
            } catch (UnknownDBException e) {
                if (log.isErrorEnabled()) {
                    log.error("Unknown database", e);
                }
                return;
            }
            try {
                db.init();
            } catch (DBException e) {
                if (log.isErrorEnabled()) {
                    log.error("Database connection can't be initialized", e);
                }
                return;
            }
            int size = insertedKeys.size();
            int batchSize = CLEANUP_INSERTED_KEYS_BATCH_SIZE;
            for (int offset = 0; size > 0; size -= batchSize) {
                int count = Math.min(size, batchSize);
                List<String> keys = insertedKeys.subList(offset, offset + count);
                db.deleteAll(table, new HashSet<String>(keys));
            }
        }
    }
}
