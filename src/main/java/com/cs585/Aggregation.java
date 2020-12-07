package com.cs585;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
        private Double sampleRate = 0.;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            parseResult = (ParseResult) ConfUtil.getClass("parseResult", conf, ParseResult.class);
            sampleRate = parseResult.sampleRate / 100.;
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
                if (aggField.aggFunName.equals("sum")){
                    thisValue = str[aggField.nColumn];
                }else if (aggField.aggFunName.equals("count")){
                    thisValue = "1";
                }
                stringOutputValue = String.join(",",
                        stringOutputValue, thisValue);
            }
            stringOutputValue = stringOutputKey.replace("\1,", "");
            outputValue.set(stringOutputValue);
            context.write(outputKey, outputValue);
        }
    }

    public static class AggReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private ParseResult parseResult;
        private Double threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            parseResult = (ParseResult) ConfUtil.getClass("parseResult", conf, ParseResult.class);
            threshold = parseResult.sampleRate / 100.;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Integer> intResults = new ArrayList<>();
            ArrayList<Double> floatResults = new ArrayList<>();
            HashMap<Integer, Integer> indexMapping = new HashMap<>();
            for (AggField aggField: parseResult.aggFields){
                if (aggField.aggFunName.equals("sum")){
                    indexMapping.put(intResults.size() + floatResults.size(),
                            floatResults.size());
                    floatResults.add(0.);
                }else if (aggField.aggFunName.equals("count")){
                    indexMapping.put(intResults.size() + floatResults.size(),
                            intResults.size());
                    intResults.add(0);
                }
            }
            String stringOutputValue = "\1";
            for (Text val : values) {
                String[] str = val.toString().split(",");
                for (int i = 0; i < parseResult.aggFields.size(); i++){
                    AggField aggField = parseResult.aggFields.get(i);
                    if (aggField.aggFunName.equals("sum")){
                        floatResults.set(indexMapping.get(i),
                                floatResults.get(indexMapping.get(i)) + Double.parseDouble(str[i])
                        );
                    }else if (aggField.aggFunName.equals("count")){
                        intResults.set(indexMapping.get(i),
                                intResults.get(indexMapping.get(i)) + Integer.parseInt(str[i])
                        );
                    }
                }
            }
            for (int i = 0; i < parseResult.aggFields.size(); i++){
                AggField aggField = parseResult.aggFields.get(i);
                if (aggField.aggFunName.equals("sum")){
                    stringOutputValue = String.join(",",
                            stringOutputValue,
                            String.valueOf(floatResults.get(indexMapping.get(i)) / threshold));
                }else if (aggField.aggFunName.equals("count")){
                    stringOutputValue = String.join(",",
                            stringOutputValue,
                            String.valueOf(Math.round(intResults.get(indexMapping.get(i)) / threshold)));
                }
            }
            stringOutputValue = stringOutputValue.replace("\1,", "");
            outputKey.set(key.toString());
            outputValue.set(stringOutputValue);
            context.write(outputKey, outputValue) ;
        }
    }

    public static boolean run(ParseResult parseResult, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String hadoop_home = System.getenv("HADOOP_HOME");
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
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

