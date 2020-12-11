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
        this(parseResult, "/tmp/cs585/tmp/aggOutput" + count,
                "/tmp/cs585/tmp/aggTmp" + count, "/tmp/cs585/tmp/cmpTmp" + count, maxIter);
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
        System.out.println("Running iter 0");
        Aggregation.run(parseResult, outputPath);
        while(iter < maxIter){
            System.out.println("Running iter " + iter);
            Aggregation.run(parseResult, aggTmp);
            CalThreshold calThreshold =
                    new CalThreshold(outputPath, aggTmp, iter + 1, this.cmpTmp, parseResult);
            double epsilon = calThreshold.giveEpsilon();
            System.out.println("\tEpsilon: " + epsilon);
            if (epsilon < parseResult.threshold){
                System.out.println("Success");
                break;
            }
            iter ++;
        }
        if (iter + 1 == maxIter){
            System.out.println("Max iter reached");
        }
        return outputPath;
    }
}
