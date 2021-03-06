package com.cs585;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Aggregation {
    private static int count = 0;

    public static class AggMapper
            extends Mapper<Object, Text, Text, Text> {

        private ParseResult parseResult;

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private Random rnd = new Random();
        private Double sampleRate;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            parseResult = (ParseResult) ConfUtil.getClass("parseResult", conf, ParseResult.class);
            sampleRate = parseResult.sampleRate;
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (rnd.nextDouble() > sampleRate){
                return ;
            }

            String[] str = value.toString().split(",");

            String stringOutputKey = "\1";
            String stringOutputValue = "\1";
            // key
            for (GroupByField groupByField: parseResult.groupByFields){
                stringOutputKey = String.join(",",
                        stringOutputKey, str[groupByField.nColumn]);
            }
            stringOutputKey = stringOutputKey.replace("\1,", "");
            outputKey.set(stringOutputKey);

            // value
            for (AggField aggField: parseResult.aggFields){
                String thisValue = "";
                switch (aggField.aggFunName) {
                    case "sum":
                        thisValue = str[aggField.nColumn];
                        break;
                    case "count":
                        thisValue = "1";
                        break;
                    case "avg":
                        thisValue = str[aggField.nColumn];
                        break;
                }
                stringOutputValue = String.join(",",
                        stringOutputValue, thisValue);
            }
            stringOutputValue = stringOutputValue.replace("\1,", "");
            outputValue.set(stringOutputValue);
            context.write(outputKey, outputValue);
        }
    }

    public static class AggReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private ParseResult parseResult;
        private Double sampleRate;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            parseResult = (ParseResult) ConfUtil.getClass("parseResult", conf, ParseResult.class);
            sampleRate = parseResult.sampleRate;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Integer> intResults = new ArrayList<>();
            ArrayList<Double> floatResults = new ArrayList<>();
            HashMap<Integer, Integer> indexMapping = new HashMap<>(); // maps total index to float or int index
            Integer count = 0;
            // int and float numbering phase
            for (AggField aggField: parseResult.aggFields){
                switch (Objects.requireNonNull(ParseResult.checkReturnType(aggField.aggFunName))) {
                    case "float":
                        indexMapping.put(intResults.size() + floatResults.size(),
                                floatResults.size());
                        floatResults.add(0.);
                        break;
                    case "int":
                        indexMapping.put(intResults.size() + floatResults.size(),
                                intResults.size());
                        intResults.add(0);
                        break;
                }
            }
            // calculating phase
            String stringOutputValue = "\1";
            for (Text val : values) {
                String[] str = val.toString().split(",");
                count += 1;
                for (int i = 0; i < parseResult.aggFields.size(); i++){
                    AggField aggField = parseResult.aggFields.get(i);
                    switch (aggField.aggFunName) {
                        case "sum":
                            floatResults.set(indexMapping.get(i),
                                    floatResults.get(indexMapping.get(i)) + Double.parseDouble(str[i])
                            );
                            break;
                        case "count":
                            intResults.set(indexMapping.get(i),
                                    intResults.get(indexMapping.get(i)) + Integer.parseInt(str[i])
                            );
                            break;
                        case "avg":
                            floatResults.set(indexMapping.get(i),
                                    floatResults.get(indexMapping.get(i)) + Double.parseDouble(str[i])
                            );
                            break;
                    }
                }
            }
            // calculating ending phase (deal with avg)
            count = (int) (count / sampleRate);
            for (int i = 0; i < parseResult.aggFields.size(); i++){
                AggField aggField = parseResult.aggFields.get(i);
                switch (aggField.aggFunName){
                    case "sum":
                        break;
                    case "count":
                        break;
                    case "avg":
                        floatResults.set(indexMapping.get(i),
                                floatResults.get(indexMapping.get(i)) / count
                        );
                        break;
                }
            }
            // constructing result phase
            for (int i = 0; i < parseResult.aggFields.size(); i++){
                AggField aggField = parseResult.aggFields.get(i);
                switch (Objects.requireNonNull(ParseResult.checkReturnType(aggField.aggFunName))) {
                    case "float":
                        stringOutputValue = String.join(",",
                                stringOutputValue,
                                String.valueOf(floatResults.get(indexMapping.get(i)) / sampleRate));
                        break;
                    case "int":
                        stringOutputValue = String.join(",",
                                stringOutputValue,
                                String.valueOf(Math.round(intResults.get(indexMapping.get(i)) / sampleRate)));
                        break;
                }
            }
            stringOutputValue = stringOutputValue.replace("\1,", "");
            outputKey.set(key.toString());
            outputValue.set(stringOutputValue);
            context.write(outputKey, outputValue) ;
        }
    }

    public static boolean run(ParseResult parseResult, Path outputPath) throws IOException, ClassNotFoundException,
            InterruptedException {
        Configuration conf = new Configuration();
        ConfUtil.setClass("parseResult", conf, parseResult);
        Job job = Job.getInstance(conf, "Agg Loop #" + count);
        job.setJarByClass(Aggregation.class);
        job.setMapperClass(AggMapper.class);
        job.setReducerClass(AggReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(parseResult.table.path));
        FileOutputFormat.setOutputPath(job, outputPath);
        count ++;
        return job.waitForCompletion(false);

    }
}

