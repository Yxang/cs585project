package com.cs585;

import org.apache.hadoop.fs.Path;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.apache.commons.lang3.math.NumberUtils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cli {
    public static void read(String path)
            throws Exception {
        System.out.println("Reading Schemas from " + path);

        File inputFile = new File(path);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(inputFile);
        doc.getDocumentElement().normalize();

        Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build();
        LineReader lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .history(new DefaultHistory())
                .build();

        NodeList nodeList = doc.getElementsByTagName("schema");
        Schemas schemas = new Schemas(nodeList);
        schemas.printAll();



        String prompt = "CS585>";
        String line;
        while (true){
            try{
                line = lineReader.readLine(prompt);
                if (line.length() == 0){
                    continue;
                }
                // Do things
                ParseResult parseResult =
                        ParseResult.parseFromString(line, schemas, new String[]{"sum", "avg", "count"});
                if (parseResult != null) {
                    submitJob(parseResult);
                }
            }
            catch (UserInterruptException e){
                // Do nothing
            }
            catch (EndOfFileException e){
                System.out.println("\nGoodbye.");
                FileOperation.deleteData(new Path("/tmp/cs585"));
                return ;
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length < 1){
            System.out.println("Please input the path of the schemas configuration file.");
        }
        else{
            String path = args[0];
            read(path);
        }
    }

    public static void submitJob(ParseResult parseResult) throws Exception {
        LoopController loopController = new LoopController(parseResult, 6);
        FinalResultWriter finalResultWriter = new FinalResultWriter(parseResult);
        Path mrOutputPath = loopController.run();
        finalResultWriter.convertToFinal(mrOutputPath, new Path(parseResult.outputPath));
    }
}

