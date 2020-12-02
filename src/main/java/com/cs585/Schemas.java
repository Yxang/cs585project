package com.cs585;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;

public class Schemas{
    private ArrayList<Schema> schemas = new ArrayList<>();
    Schemas(){}
    Schemas(NodeList nodeList) throws IOException {
        // Init hadoop conf
        Configuration conf = new Configuration();
        String hadoop_home = System.getenv("HADOOP_HOME");
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        FileSystem fs = FileSystem.get(conf);

        for (int i = 0; i < nodeList.getLength(); i ++){
            Element node = (Element) nodeList.item(i);
            String name = node.getAttribute("name");
            String stringPath = node.getElementsByTagName("path").item(0).getTextContent().trim();
            Path path = new Path(node.getElementsByTagName("path").item(0).getTextContent().trim());
            if (!fs.exists(path)) {
                System.out.println("Schema \""
                        + name
                        + "\" not found at path: \n"
                        + path);
                continue;
            }
            schemas.add(new Schema(node, stringPath));
        }
    }

    public Schema getByName(String schemaName){
        for (Schema schema: schemas){
            if (schema.isName(schemaName)){
                return schema;
            }
        }
        return null;
    }

    public void printAll(){
        for (Schema schema: schemas){
            schema.print();
        }
    }
}

