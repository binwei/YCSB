package com.yahoo.ycsb.hbase;

public interface HBaseClientProperties {

    final String DEBUG_PROPERTY = "debug";

    final String TABLE_PROPERTY = "table";

    final String TABLE_PROPERTY_DEFAULT = "t1";

    final String COLUMN_FAMILY_PROPERTY = "columnfamily";

    final String COLUMN_FAMILY_PROPERTY_DEFAULT = "f1";

    final int WRITE_BUFFER_SIZE = 1024 * 1024 * 12;
}
