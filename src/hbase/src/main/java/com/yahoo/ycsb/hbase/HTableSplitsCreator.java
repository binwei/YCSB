package com.yahoo.ycsb.hbase;

import com.yahoo.ycsb.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Properties;

public class HTableSplitsCreator implements HBaseClientProperties {

    public static final String REGION_COUNT_PROPERTY = "regioncount";

    public static final String CONFIGURATION_FILE = "/region/hbase-site.xml";

    private static Log log = LogFactory.getLog(HTableSplitsCreator.class);

    private int regionCount;

    private int recordCount;

    private String table;

    private String columnFamily;

    private HBaseAdmin getAdmin() throws MasterNotRunningException, ZooKeeperConnectionException {
        Configuration config = HBaseConfiguration.create();
        config.addResource(CONFIGURATION_FILE);
        return new HBaseAdmin(config);
    }

    private HTableDescriptor getTable() {
        HTableDescriptor table = new HTableDescriptor(this.table);
        table.addFamily(new HColumnDescriptor(this.columnFamily));
        return table;
    }

    private static boolean createTable(HBaseAdmin admin, HTableDescriptor table, byte[][] splits) throws IOException {
        try {
            admin.createTable(table, splits);
            return true;
        } catch (TableExistsException e) {
            log.info("Table " + table.getNameAsString() + " already exists");
            return false;
        }
    }

    private static byte[][] getHexSplits(long startKey, long endKey, int regions) {
        byte[][] splits = new byte[regions - 1][];
        BigInteger lowestKey = new BigInteger(Long.toString(startKey));
        BigInteger highestKey = new BigInteger(Long.toString(endKey));
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(regions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < regions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    public void create() throws IOException {
        byte[][] hexSplits = getHexSplits(0, recordCount, regionCount);
        createTable(getAdmin(), getTable(), hexSplits);
    }

    public void init(Properties properties) throws IOException {
        String recordCount = properties.getProperty(Client.RECORD_COUNT_PROPERTY);
        if (recordCount == null) {
            throw new IllegalArgumentException(String.format("Property %1$s is required", Client.RECORD_COUNT_PROPERTY));
        }
        this.recordCount = Integer.parseInt(recordCount);
        String regionCount = properties.getProperty(REGION_COUNT_PROPERTY);
        if (regionCount == null) {
            throw new IllegalArgumentException(String.format("Property %1$s is required", REGION_COUNT_PROPERTY));
        }
        this.regionCount = Integer.parseInt(regionCount);
        this.table = properties.getProperty(TABLE_PROPERTY, TABLE_PROPERTY_DEFAULT);
        this.columnFamily = properties.getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
    }

    public static void main(String[] args) throws IOException {
        String file = args[0];
        Properties properties = new Properties();
        properties.load(new FileInputStream(file));

        HTableSplitsCreator creator = new HTableSplitsCreator();
        creator.init(properties);
        creator.create();
    }
}
