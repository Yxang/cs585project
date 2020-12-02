package com.cs585;

public class GroupByField {
    public String name;
    public Integer nColumn;

    GroupByField() {
    }

    GroupByField(String name, Integer nColumn) {
        this.name = name;
        this.nColumn = nColumn;
    }

    @Override
    public String toString() {
        return "GroupByFields{" +
                "name='" + name + '\'' +
                ", nColumn=" + nColumn +
                '}';
    }
}
