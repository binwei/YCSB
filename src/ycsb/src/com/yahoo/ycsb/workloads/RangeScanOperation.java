package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RangeScanOperation {

    int scan(String table, String startKey, String endKey, Set<String> fields, List<Map<String, ByteIterator>> results);
}
