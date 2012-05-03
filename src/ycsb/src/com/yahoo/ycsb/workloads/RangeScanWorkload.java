package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class RangeScanWorkload extends CoreWorkload {

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
        ((RangeScanOperation)db).scan(table, buildKeyName(startKey), buildKeyName(endKey), fields, new Vector<Map<String, ByteIterator>>());
    }
}
