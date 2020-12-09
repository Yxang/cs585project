package com.cs585;

import java.util.ArrayList;
import java.util.HashMap;

public class ParseResult{
    public ArrayList<GroupByField> groupByFields;
    public ArrayList<AggField> aggFields;
    public HashMap<Integer, FieldLocation> locationMap;
    public Schema table;
    public Integer threshold;
    public Integer sampleRate;
    ParseResult(){}
    ParseResult(ArrayList<GroupByField> groupByFields, ArrayList<AggField> aggFields,
                HashMap<Integer, FieldLocation> locationMap,
                Schema table, Integer threshold,
                Integer sampleRate){
        this.groupByFields = groupByFields;
        this.aggFields = aggFields;
        this.locationMap = locationMap;
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

class FieldLocation{
    public String type;
    public Integer index;
    FieldLocation(){}
    FieldLocation(String type, Integer index){
        assert type.equals("agg") || type.equals("non-agg") : "type not should be \"agg\" or \"non-agg\"";
        this.type = type;
        this.index = index;
    }
}
