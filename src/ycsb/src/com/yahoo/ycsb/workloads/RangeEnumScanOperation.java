package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RangeEnumScanOperation {

    int scan(String table, List<String> keys, Set<String> fields, List<Map<String, ByteIterator>> results);
}
