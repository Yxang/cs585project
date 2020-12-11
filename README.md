# CS585/DS503 Project 4

This is the repository for Course CS585/DS503 Big Data Management, 2020 Fall, Project 4: Progressive Approximate Answers for Aggregation Queries. For the details, please see the reports.

Author: Xinyuan Yang, Enze Liu

## Compilation

We use maven to manage the project. To build the project, please just clone the repository and build.

```bash
git clone https://github.com/Yxang/cs585project.git
cd cs585project
mvn package
```

You can find the jar file in target. We recommend you to use the jar with dependencies because the program relies on several nonstandard packages.

## Usage

We use Hadoop 3 APIs and the program is tested on Hadoop 3.1.3. We do not guareentee the result on Hadoop 2.

### Start the program

When you start the program, you have to provide it with a xml file of the schemas information. One sample file of the databases used for project 1 is in input/SchemaInfo.xml. You have to make sure the text file for the tables are correctly at the paths in xml file, and the program will only warn you when it detect no files. Once you have that, type something like this to start

```bash
hadoop jar target/cs585project-1.0-SNAPSHOT-jar-with-dependencies.jar input/SchemaInfo.xml
```
Once you start the program it will read the xml file for the schemas and print the information it reads. Once its done, you can excecute SQL or exit. Here is a sample output from `input/SchemaInfo.xml`

```
$ hadoop jar target/cs585project-1.0-SNAPSHOT-jar-with-dependencies.jar input/SchemaInfo.xml
Reading Schemas from SampleFile.xml
Transaction
        TransID int
        CustID  int
        TransTotal      float
        TransNumItems   int
        TransDesc       str
Customers
        ID      int
        Name    str
        Age     int
        Gender  str
        CountryCode     int
        Salary  float
CS585>
```

Then you can input commands.

### Execute SQL

The format of the SQL command should be

```
SELECT Field,aggFun(Field),...
FROM Table
GROUP BY Field,...
[ THRESHOLD t]
[ SAMPLE RATE r]
[ PATH /path/to/output/file]
```

We support three aggregation function: sum, count and avg.

Different from the convention that `;` marks the end of the statement, we take a whole line as a statement, so please DO NOT type `;` at the end, and type the SQL all In ONE LINE. The command is case insensitive. 

This command will execute the progressive aggregation jobs with threshold t, and sample rate r. The result will be output to both the console and saved to `/path/to/output/file`.

### Exit

One can type `exit` or just press Ctrl-D to exit the program.
