package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class RangeEnumScanWorkload extends CoreWorkload {

	@Override
	public void doTransactionScan(DB db) {
		int length = scanlength.nextInt();
		List<String> keys = new ArrayList<String>();
		int startKey = nextKeynum();
		int endKey = startKey + length;
		for (int keyNum = startKey; keyNum <= endKey; keyNum++) {
			keys.add(buildKeyName(keyNum));
		}
		Set<String> fields = null;
		if (!readallfields) {
			// read a random field
			String field = "field" + fieldchooser.nextString();
			fields = new HashSet<String>();
			fields.add(field);
		}
		((RangeEnumScanOperation) db).scan(table, keys, fields, new Vector<Map<String, ByteIterator>>());
	}
}
