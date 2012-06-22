package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBWrapper;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RangeScanWorkload extends CoreWorkload {

    public static final String RANGE_SCAN = "RANGE-SCAN";

    @Override
    public void doTransactionScan(DB db) {
        int startKey = nextKeynum();
        int length = scanlength.nextInt();
        int endKey = startKey + length;
        Set<String> fields = null;
        if (!readallfields) {
            //read a random field
            String field = "field" + fieldchooser.nextString();
            fields = new HashSet<String>();
            fields.add(field);
        }
        DBWrapper wrapper = (DBWrapper)db;
        RangeScanOperation operation = (RangeScanOperation) wrapper.getDB();
        long start = System.nanoTime();
        int result = operation.scan(table, buildKeyName(startKey), buildKeyName(endKey),
                length, fields, new ArrayList<Map<String, ByteIterator>>());
        long end = System.nanoTime();
        Measurements measurements = wrapper.getMeasurements();
        measurements.measure(RANGE_SCAN, (int) ((end - start) / 1000));
        measurements.reportReturnCode(RANGE_SCAN, result);
    }
}
