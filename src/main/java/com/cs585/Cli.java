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
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cli {
    public static void read(String path)
            throws IOException, ParserConfigurationException, SAXException,
            InterruptedException, ClassNotFoundException {
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
                        parse(line, schemas, new String[]{"sum", "avg", "count"});
                if (parseResult != null) {
                    core(parseResult);
                    Aggregation.run(parseResult, new Path("output"));
                }
            }
            catch (UserInterruptException e){
                // Do nothing
            }
            catch (EndOfFileException e){
                System.out.println("\nGoodbye.");
                return ;
            }
        }
    }

    public static void main(String[] args)
            throws IOException, ParserConfigurationException, SAXException,
            ClassNotFoundException, InterruptedException {

        if (args.length < 1){
            System.out.println("Please input the path of the schemas configuration file.");
        }
        else{
            String path = args[0];
            read(path);
        }
    }

    private static ParseResult parse(String input, Schemas schemas, String[] supportedAggFun){
        ArrayList<String> arraySupportedAggFun = new ArrayList<>(Arrays.asList(supportedAggFun));
        // 0: group by columns
        // 1: agg columns to apply functions
        // 2: from what table
        // 3: threshold
        ParseResult result = null;

        String pattern = "\\Aselect ([\\S(?<=,) ]+?) from ([\\S(?<=,) ]+?) group by ([\\S(?<=,) ]+?)(?: threshold ([\\S(?<=,) ]+))?\\Z";
        Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(input.trim());
        if (m.find()){
            String select = m.group(1);
            String from = m.group(2);
            String groupBy = m.group(3);
            String threshold = m.group(4);  // null if not exists

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
            Integer intThreshold = Integer.parseInt(threshold);



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
            ArrayList<AggField> aggItems = new ArrayList<>();
            String aggPattern = "\\A(.*)\\((.+)\\)\\Z";
            Pattern aggr = Pattern.compile(aggPattern, Pattern.CASE_INSENSITIVE);
            Matcher aggm;
            for (String selectItem : selectItems){
                selectItem = selectItem.trim();
                String realSelectItem;
                String functionName;
                aggm = aggr.matcher(selectItem);
                if (aggm.find()) {
                    // Function form
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

            result = new ParseResult(groupByItems, aggItems, fromSchema, intThreshold);
        }
        else{
            System.out.println("Invalid command.");
        }
        return result;
    }
    public static boolean isNumeric(String str) {
        return str != null && str.matches("[-+]?\\d*\\.?\\d+");
    }

    public static void core(ParseResult parseResult){
        parseResult.print();
    }
}

