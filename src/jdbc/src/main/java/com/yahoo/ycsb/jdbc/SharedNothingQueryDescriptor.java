package com.yahoo.ycsb.jdbc;

public class SharedNothingQueryDescriptor implements QueryDescriptor {
    private QueryType type;
    private String table;
    private int parametersCount;

    public SharedNothingQueryDescriptor(QueryType type, String table, int parametersCount) {
        this.type = type;
        this.table = table;
        this.parametersCount = parametersCount;
    }

    public QueryType getType() {
        return type;
    }

    public String getTable() {
        return table;
    }

    public int getParameters() {
        return parametersCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SharedNothingQueryDescriptor that = (SharedNothingQueryDescriptor) o;

        if (parametersCount != that.parametersCount) return false;
        if (table != null ? !table.equals(that.table) : that.table != null) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + parametersCount;
        return result;
    }
}