package com.cs585;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseResult{
    private static int count = 0;
    public ArrayList<GroupByField> groupByFields;
    public ArrayList<AggField> aggFields;
    public HashMap<Integer, FieldLocation> locationMap;
    public Schema table;
    public Double threshold;
    public Double sampleRate;
    public String outputPath;
    ParseResult(){}
    ParseResult(ArrayList<GroupByField> groupByFields,
                ArrayList<AggField> aggFields,
                HashMap<Integer, FieldLocation> locationMap,
                Schema table,
                Double threshold,
                Double sampleRate,
                String outputPath){
        this.groupByFields = groupByFields;
        this.aggFields = aggFields;
        this.locationMap = locationMap;
        this.table = table;
        this.threshold = threshold / 100.;
        this.sampleRate = sampleRate / 100.;
        this.outputPath = outputPath;
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
        System.out.println("Received location map: ");
        for (Integer i: locationMap.keySet()){
            System.out.println(i + ": " + locationMap.get(i));
        }
        System.out.println("Received target table " + table.name);
        System.out.println("Received threshold " + threshold);
        System.out.println("Received sample rate " + sampleRate);
    }
    public static ParseResult parseFromString(@org.jetbrains.annotations.NotNull String input,
                                              Schemas schemas, String[] supportedAggFun)
            throws ValueNotFoundException, IOException {
        ArrayList<String> arraySupportedAggFun = new ArrayList<>(Arrays.asList(supportedAggFun));
        // 0: group by columns
        // 1: agg columns to apply functions
        // 2: from what table
        // 3: threshold
        ParseResult result = null;

        String pattern = "\\Aselect ([\\S(?<=,) ]+?) from ([\\S(?<=,) ]+?) group by ([\\S(?<=,) ]+?)" +
                "(?: threshold ([\\S(?<=,) ]+?))?" +
                "(?: sample rate ([\\S(?<=,) ]+?))?" +
                "(?: path \"(.+?)\")?\\Z";
        Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(input.trim());
        if (m.find()){
            String select = m.group(1);
            String from = m.group(2);
            String groupBy = m.group(3);
            String threshold = m.group(4);  // null if not exists
            String sampleRate = m.group(5);  // null if not exists
            String path = m.group(6); // null if not exists

            // threshold
            Double doubleThreshold = parseThreshold(threshold);
            if (doubleThreshold == null){
                return null;
            }

            // sample rate
            Double doubleSampleRate = parseSampleRate(sampleRate);
            if (doubleSampleRate == null){
                return null;
            }

            // path
            if (path == null){
                path = "/tmp/cs585/finalOutput" + count;
                System.out.println("Result will be stored at " + path);
            }
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(new Path(path))) {
                System.out.println("Path \"" + path +"\" is not empty");
                return null;
            }else{
                count ++;
            }


            // "from" should be in the schemas
            Schema fromSchema = schemas.getByName(from);
            if (fromSchema == null){
                System.out.println("Schema \"" + from + "\" does not exists.");
                return null;
            }

            // "group by" should be in the fields of the schemas
            ArrayList<GroupByField> groupByItems = new ArrayList<>();
            ArrayList<String> groupByItemsString = new ArrayList<>();

            for (String groupByItem: groupBy.toLowerCase().trim().split(",")){
                if (!fromSchema.hasField(groupByItem)) {
                    System.out.println("Group by field \""
                            + groupByItem
                            + "\" not in schema \""
                            + fromSchema.name
                            + "\".");
                    return null;
                }
                groupByItems.add(new GroupByField(groupByItem, fromSchema.getFieldNumber(groupByItem)));
                groupByItemsString.add(groupByItem);
            }


            // "select" should be in the fields of the schemas, and function in supportedAggFun
            ArrayList<String> selectItems = new ArrayList<>(
                    Arrays.asList(select.toLowerCase().trim().split(","))
            );
            HashMap<Integer, FieldLocation> locationMap = new HashMap<>();
            ArrayList<AggField> aggItems = new ArrayList<>();
            String aggPattern = "\\A(.*)\\((.+)\\)\\Z";
            Pattern aggr = Pattern.compile(aggPattern, Pattern.CASE_INSENSITIVE);
            Matcher aggm;
            for (int i = 0, agg_i = 0, non_agg_i = 0; i < selectItems.size(); i++){
                String selectItem = selectItems.get(i);
                selectItem = selectItem.trim();
                String realSelectItem;
                String functionName;
                aggm = aggr.matcher(selectItem);
                if (aggm.find()) {
                    // agg field
                    locationMap.put(i, new FieldLocation("agg", agg_i++));
                    functionName = aggm.group(1);
                    realSelectItem = aggm.group(2);
                    if (!arraySupportedAggFun.contains(functionName)){
                        System.out.println("Agg function \"" + functionName + "\" unknown");
                        return null;
                    }
                    if (!fromSchema.hasField(realSelectItem)){
                        System.out.println("Select by field \""
                                + realSelectItem
                                + "\" not in schema \""
                                + fromSchema.name
                                + "\".");
                        return null;
                    }
                    if (!functionName.equals("count") && !fromSchema.hasNumericField(realSelectItem)){
                        System.out.println("Unsupported data type of field \""
                                + realSelectItem
                                + "\" for agg function \""
                                + functionName
                                + "\".");
                        return null;
                    }
                    aggItems.add(new AggField(realSelectItem, functionName, fromSchema.getFieldNumber(realSelectItem)));
                }
                else{
                    // group by field
                    locationMap.put(i, new FieldLocation("non-agg", non_agg_i++));
                    realSelectItem = selectItem;
                    if (!fromSchema.hasField(realSelectItem)){
                        System.out.println("Select by field \""
                                + realSelectItem
                                + "\" not in schema \""
                                + fromSchema.name
                                + "\".");
                        return null;
                    }
                    if (!groupByItemsString.contains(selectItem)){
                        System.out.println("No agg function for \""
                                + realSelectItem
                                + "\".");
                        return null;
                    }
                }
            }
            if (aggItems.size() == 0){
                System.out.println("At least one agg field needed.");
                return null;
            }

            result = new ParseResult(groupByItems,
                    aggItems,
                    locationMap,
                    fromSchema,
                    doubleThreshold,
                    doubleSampleRate,
                    path);
        }
        else{
            System.out.println("Invalid command.");
        }
        return result;
    }

    private static boolean isNumeric(String str) {
        return NumberUtils.isNumber(str);
    }

    private static Double parseThreshold(String threshold){
        if (threshold == null){
            threshold = "5";
        }
        else{
            if (!isNumeric(threshold)){
                System.out.println("Threshold \"" + threshold + "\" is not numeric");
                return null;
            }
            else if (Double.parseDouble(threshold) < 0 || Double.parseDouble(threshold) > 100){
                System.out.println("Threshold \"" + threshold + "\" should between 0 and 100");
                return null;
            }
        }
        return Double.parseDouble(threshold);
    }

    private static Double parseSampleRate(String sampleRate){
        if (sampleRate == null){
            sampleRate = "100";
        }
        else{
            if (!isNumeric(sampleRate)){
                System.out.println("Sample rate \"" + sampleRate + "\" is not numeric");
                return null;
            }
            else if (Double.parseDouble(sampleRate) < 0 || Double.parseDouble(sampleRate) > 100){
                System.out.println("Sample rate \"" + sampleRate + "\" should between 0 and 100");
                return null;
            }
        }
        return Double.parseDouble(sampleRate);
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
