# avrocli

AVRO CLI is a command line troubleshooting tool to view and query data in AVRO files located in both local and hdfs file system. 

## Getting started

```bash

Run the script bin/avrocli

The available commands are listed below: 

1. help	               : Displays available commands
   help <command_name> : Displays information on the specific command
   Example	       : help describe

2. describe            : Describes the schema for a given Avro file 
   Usage               : describe <[hdfs:|file:]Avro file Name> 
   Example             : describe hdfs:/var/tmp/test.avro 
                         describe file:/var/tmp/test.avro

3. bulkAvroToCsv       : Converts AVRO files to CSV files 
   Usage               : bulkAvroToCsv <[hdfs:|file:]Avro SRC dir> <CSV DEST dir> <threadCount> 
   Example             : bulkAvroToCsv hdfs:/var/tmp/ /var/tmp/backup/ 6

3. select              : Select the records from given AVRO file either from localfile system(give file:) or hdfs(give hdfs:)
   Usage               : select <[column_name|operation_on_column]> from <[hdfs:|file:]Avro file Name> where <column_name>=<value> and/or .. groupby(<column_name)
   
Operations supported on column
------------------------------
1.sum(<column_name>)      : gives sum of the given column
2.min(<column_name>)      : gives min of the given column
3.max(<column_name>)      : gives max of the given column
4.distinct(<column_name>) : gives distinct of the given column(this operation is supported only in mapreduce for now)
5.groupby(<column_name>   : gives output based on the groupby column(this operation is supported only in mapreduce for now)
 
   Example 1           : select * from hdfs:/var/tmp/test.avro
			 select accountNumber,balance from hdfs:/var/tmp/test.avro where msisdn=1234
			 select count(*) from hdfs:/var/tmp/test.avro

   Example 2	       : select sum(creditAmount) from hdfs:/var/tmp/test.avro where serviceClass<150 and drType=6
			 select sum(creditAmount) from hdfs:/var/tmp/test* where serviceClass<150 and drType=6 groupby(journalTypeId)
NOTE :
------
   1. select statement works in two modes. Local job(single jvm) or a mapreduce job. To select mutliple files give * as regex. (See Example 2). When multiple files are selected       a mapreduce job is automatically triggered. For a Local job give only single file (see Example 1)
   2. Filter conditions supported are AND and OR only and the operators supported are = , != , > , < , >= and <=. 
   3. Substructures filtering are not supported. 

4. exit                : Exits AvroReader 


```



## Maven Usage

Use the following definition to use `sqlline` in your maven project:

```xml
<dependency>
  <groupId>avrocli</groupId>
  <artifactId>avrocli</artifactId>
  <version>1.0</version>
</dependency>
```

## Building

Prerequisites:

* Maven 2+
* Java 5+

Check out and build:

```bash
git clone https://github.com/PrabhuJoseph/avrocli.git
cd avrocli
mvn package
```

## Authors

* [Prabhu Joseph](https://github.com/PrabhuJoseph)

