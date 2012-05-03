package com.yahoo.ycsb.jdbc;

public class ShardedQueryDescriptor implements QueryDescriptor {

    private QueryType type;
    private String table;
    private int shard;
    private int parameters;

    public ShardedQueryDescriptor(QueryType type, String table, int shard, int parameters) {
        this.type = type;
        this.table = table;
        this.shard = shard;
        this.parameters = parameters;
    }

    public QueryType getType() {
        return type;
    }

    public String getTable() {
        return table;
    }

    public int getParameters() {
        return parameters;
    }

    public int getShard() {
        return shard;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardedQueryDescriptor that = (ShardedQueryDescriptor) o;

        if (shard != that.shard) return false;
        if (parameters != that.parameters) return false;
        if (table != null ? !table.equals(that.table) : that.table != null) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = shard;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + parameters;
        return result;
    }
}
