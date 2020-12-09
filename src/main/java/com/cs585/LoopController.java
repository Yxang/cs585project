package com.cs585;

import org.apache.hadoop.fs.Path;

public class LoopController {
    private static int count=0;
    private ParseResult parseResult;
    private Path outputPath;
    private Path aggTmp;
    private Path cmpTmp;
    private Integer maxIter;
    LoopController(ParseResult parseResult, Integer maxIter){
        this(parseResult, "/tmp/cs585/aggOutput" + count,
                "/tmp/cs585/aggTmp" + count, "/tmp/cs585/cmpTmp" + count, maxIter);
    }
    LoopController(ParseResult parseResult, String outputPath, String aggTmp, String cmpTmp, Integer maxIter){
        this.parseResult = parseResult;
        this.outputPath = new Path(outputPath);
        this.aggTmp = new Path(aggTmp);
        this.cmpTmp = new Path(cmpTmp);
        this.maxIter = maxIter;
        count ++;
    }
    public Path run() throws Exception {
        int iter = 1;
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Running iter 0");
        Aggregation.run(parseResult, outputPath);
        while(iter < maxIter){
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Running iter " + iter);
            Aggregation.run(parseResult, aggTmp);
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Calculating threshold");
            CalThreshold calThreshold = new CalThreshold(outputPath, aggTmp, iter + 1, this.cmpTmp);
            double threshold = calThreshold.giveThreshold();
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>The threshold is: " + threshold);
            if (calThreshold.giveThreshold() < parseResult.threshold / 100.){
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Success");
                break;
            }
            iter ++;
        }
        if (iter == maxIter){
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Max iter reached");
        }
        return outputPath;
    }
}
