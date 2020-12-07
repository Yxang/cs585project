package com.cs585;

import org.apache.hadoop.fs.Path;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;

class Schema {

    private static class Field{
        String name;
        String type;
        Field(){}
        Field(String name, String type) {
            this.name = name.toLowerCase();
            this.type = type.toLowerCase();
        }
    }

    public String name;
    public ArrayList<Field> fields = new ArrayList<>();
    public String path;

    Schema(){}

    Schema(Element element, String path){
        this.name = element.getAttribute("name");
        this.path = path;
        NodeList fieldNodeList = element.getElementsByTagName("field");
        for (int i = 0; i < fieldNodeList.getLength(); i ++){
            Node nNode = fieldNodeList.item(i);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                String fieldName = eElement.getElementsByTagName("name").item(0).getTextContent();
                String fieldType = eElement.getElementsByTagName("dtype").item(0).getTextContent();
                fields.add(new Field(fieldName, fieldType));
            }
        }
    }

    Schema(Element element) {
        this.path = element.getElementsByTagName("path").item(0).getTextContent().trim();

        this.name = element.getAttribute("name");
        NodeList fieldNodeList = element.getElementsByTagName("field");
        for (int i = 0; i < fieldNodeList.getLength(); i ++){
            Node nNode = fieldNodeList.item(i);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                String fieldName = eElement.getElementsByTagName("name").item(0).getTextContent();
                String fieldType = eElement.getElementsByTagName("dtype").item(0).getTextContent();
                fields.add(new Field(fieldName, fieldType));
            }
        }
    }

    public void print(){
        System.out.println(this.name);
        for (Field field: fields){
            System.out.println("\t"
                    + field.name
                    + "\t"
                    + field.type);
        }
    }

    public boolean isName(String name){
        return name.toLowerCase().equals(this.name.toLowerCase());
    }

    public boolean hasField(String name) {
        for (Field field: fields){
            if (field.name.toLowerCase().equals(name.toLowerCase())){
                return true;
            }
        }
        return false;
    }

    public boolean hasNumericField(String name) {
        for (Field field: fields){
            if (field.name.toLowerCase().equals(name.toLowerCase())
                    && !field.type.equals("str")){
                return true;
            }
        }
        return false;
    }

    public Integer getFieldNumber(String name){
        int n = 0;
        for (;n < fields.size(); n++){
            if (name.equals(fields.get(n).name)){
                return n;
            }
        }
        return -1;
    }
}
