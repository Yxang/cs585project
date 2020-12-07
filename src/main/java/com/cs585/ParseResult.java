package com.cs585;

import java.util.ArrayList;

public class ParseResult{
    public ArrayList<GroupByField> groupByFields;
    public ArrayList<AggField> aggFields;
    public Schema table;
    public Integer threshold;
    public Integer sampleRate;
    ParseResult(){}
    ParseResult(ArrayList<GroupByField> groupByFields, ArrayList<AggField> aggFields, Schema table, Integer threshold,
                Integer sampleRate){
        this.groupByFields = groupByFields;
        this.aggFields = aggFields;
        this.table = table;
        this.threshold = threshold;
        this.sampleRate = sampleRate;
    }
    public void print(){
        System.out.println("Received groupByFields:");
        for (GroupByField groupByField: groupByFields){
            System.out.println(groupByField.toString());
        }
        System.out.println("Received aggFields:");
        for (AggField aggField: aggFields){
            System.out.println(aggField.toString());
        }
        System.out.println("Received target table " + table.name);
        System.out.println("Received threshold " + threshold);
        System.out.println("Received sample rate " + sampleRate);
    }
}

