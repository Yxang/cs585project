package com.cs585;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class FinalResultWriter {
    private ParseResult parseResult;
    FinalResultWriter(ParseResult parseResult){
        this.parseResult = parseResult;
    }

    public void convertToFinal(Path mrOutputPath, Path outputPath) throws IOException, URISyntaxException {
        System.out.println("--------------------");
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(mrOutputPath);
        FSDataOutputStream out = fs.create(outputPath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(out));
        try{
            for (FileStatus fileStatus: fileStatuses){
                Path mrFilePath = fileStatus.getPath();
                FSDataInputStream in = fs.open(mrFilePath);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
                String s;
                try{
                    while ((s = bufferedReader.readLine()) != null) {
                        String[] splitString = s.split("\t");
                        String[] groupByString = splitString[0].split(",");
                        String[] aggString = splitString[1].split(",");
                        StringBuilder output = new StringBuilder();
                        for (Integer i = 0; i < parseResult.locationMap.size(); i++){
                            FieldLocation fieldLocation = parseResult.locationMap.get(i);
                            if (fieldLocation.type.equals("agg")){
                                output.append(aggString[fieldLocation.index]);
                            }else{
                                output.append(groupByString[fieldLocation.index]);
                            }
                            output.append(",");
                        }
                        output.deleteCharAt(output.length() - 1);
                        bufferedWriter.write(output.toString());
                        bufferedWriter.newLine();
                        System.out.println(output.toString());
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    in.close();
                    bufferedReader.close();
                }
            }
            bufferedWriter.flush();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            out.close();
            bufferedWriter.close();
            FileOperation.deleteData(new Path("/tmp/cs585/tmp"));
            System.out.println("--------------------");
        }
    }
}
