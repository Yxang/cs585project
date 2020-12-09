package com.cs585;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CalThreshold {
    int sampleRound;
    Path thresholdPath;
    String HDFS_PATH = "hdfs://localhost:9000";

    CalThreshold(Path lastResult, Path thisResult, int sampleR, Path outResult) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        sampleRound = sampleR;
        thresholdPath = lastResult;
        newResult( lastResult, thisResult,  outResult);
        FileOperation operation = new FileOperation();
        operation.deleteData(HDFS_PATH, lastResult);
        operation.deleteData(HDFS_PATH, thisResult);
        operation.Rename(HDFS_PATH, outResult, lastResult);
    }

    public double giveThreshold(){
        Configuration conf = new Configuration();
        double MaxThreshold = 0;
        Path path = new Path(HDFS_PATH + thresholdPath + "/part-r-00000");
        try {
            FileSystem fileSystem = path.getFileSystem(conf);
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            BufferedReader bufr = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String s = null;
            while((s = bufr.readLine()) != null) {
                String[] str = s.split("\t");
                double Threshold = Float.parseFloat(str[2]);
                MaxThreshold = Math.max(MaxThreshold, Threshold);
            }
            bufr.close();
            fsDataInputStream.close();
            fileSystem.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
        return MaxThreshold;
    }

    public boolean newResult(Path lastResult, Path thisResult, Path outResult) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("sampleRound", String.valueOf(sampleRound));
        Job job = Job.getInstance(conf, "Calculate Threshold #" + sampleRound);
        job.setJarByClass(CalThreshold.class);
        job.setReducerClass(ProbReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,lastResult,TextInputFormat.class,lastResultMapper.class);
        MultipleInputs.addInputPath(job,thisResult,TextInputFormat.class,thisResultMapper.class);
        FileOutputFormat.setOutputPath(job, outResult);
        return job.waitForCompletion(true);
    }

    public static class lastResultMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text output_key = new Text();
        private Text output_value = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
            String groupId = str[0];
            String aggResult = str[1];
            output_key.set(groupId);
            output_value.set(String.join(",", "lastResult", aggResult));
            context.write(output_key, output_value);
        }
    }

    public static class thisResultMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text output_key = new Text();
        private Text output_value = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
            String groupId = str[0];
            String aggResult = str[1];
            output_key.set(groupId);
            output_value.set(String.join(",", "thisResult", aggResult));
            context.write(output_key, output_value);
        }
    }

    public static class ProbReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text output_Key = new Text();
        private Text output_value = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Float> calculator = new ArrayList<Float>();
            ArrayList<Float> oldResult = new ArrayList<Float>();
            int round = Integer.parseInt(context.getConfiguration().get("sampleRound"));
            for (Text val : values) {

                String[] str = val.toString().split(",");
                int aggNum = str.length - 1;
                if(calculator.size() == 0){
                    for(int i =0; i < aggNum; i++){
                        calculator.add((float) 0);
                        oldResult.add((float) 1);
                    }
                }

                if (str[0].equals("lastResult")){
                    for(int i =0; i < aggNum; i++){
                        float lastCal = (Float.parseFloat(str[i+1]) * ((float)round / (float)(round+1))) + calculator.get(i);
                        oldResult.set(i, Float.parseFloat(str[i+1]));
                        calculator.set(i,lastCal);

                    }
                }
                else if (str[0].equals("thisResult")) {
                    for(int i =0; i < aggNum; i++){
                        float thisCal = (Float.parseFloat(str[i+1]) * ((float)1 / (float)(round+1))) + calculator.get(i);
                        calculator.set(i,thisCal);
                    }
                }

            }
            float maxThreshold = 0;
            for(int k = 0; k < oldResult.size(); k++){
                float Threshold = Math.abs(calculator.get(k) / oldResult.get(k)-1);
                maxThreshold = Math.max(maxThreshold, Threshold);
            }


            output_Key.set(key);
            String output = String.valueOf(calculator.get(0));
            for(int j = 1; j < calculator.size(); j++){
                output += "," + String.valueOf(calculator.get(j));
            }
            output += "\t" + String.valueOf(maxThreshold);
            output_value.set(output);
            context.write(output_Key, output_value);
        }
    }
}
