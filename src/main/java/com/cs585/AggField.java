package com.cs585;

public class AggField {
    public String name;
    public String aggFunName;
    public Integer nColumn;

    AggField() {
    }

    AggField(String name, String aggFunName, Integer nColumn) {
        this.name = name;
        this.aggFunName = aggFunName;
        this.nColumn = nColumn;
    }

    @Override
    public String toString() {
        return "AggFields{" +
                "name='" + name + '\'' +
                ", aggFunName='" + aggFunName + '\'' +
                ", nColumn=" + nColumn +
                '}';
    }
}
