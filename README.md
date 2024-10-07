# Snowpark Hadoop Sequence File Reader

Small snippet to access a Hadoop Sequence File in Snowpark.

It implements an UDTF (User Defined Table Function) that reads a Sequence File and returns its content as rows.

# Build

```
mvn clean package   
```

That will produce a jar in the target directory. For example: `target/sequentialreader-0.0.2-jar-with-dependencies.jar``

Just in case I have uploaded compiled jar into the github releases

# Usage

Upload the jar to your Snowflake account, into a selected stage.
For example if your stage is named `mystage`:

You can register the UDTF like:

```
CREATE OR REPLACE FUNCTION HADOOP_READ(filename String)
RETURNS TABLE(
receiverId String,
parentReceiverId String,
receiverModel String,
equipmentModel String,
accountId String,
dma String,
postalCode String,
provisionedLocal String,
receivers String,
serviceCodes String,
unresolvedToken String, 
accountType String,
unresolvedTokenPayloadConfirmationCode String,
receiverInstallDate String, 
payloadArray Array,
inputPath String)
LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = ('@mystage/sequentialreader-0.0.1-jar-with-dependencies.jar')
  PACKAGES = ('com.snowflake:snowpark:latest')
  HANDLER = 'hdfs.SequenceFileReader';

```

To use the UDTF:

```sql
select * from table(HADOOP_READ('@mystage/part-m-00000'))
```


The example above reads a sequence file from HDFS and returns a table for an specific set of columns. 
If you dont know which columns upfront you can then use the GenericSequenceFileReader to read the entire file and return the key and content as a table.

```sql
CREATE OR REPLACE FUNCTION HADOOP_READ_GENERIC(filename String)
RETURNS TABLE(
key String,
content String)
LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = ('@mystage/sequentialreader-0.0.2-jar-with-dependencies.jar')
  PACKAGES = ('com.snowflake:snowpark:latest')
  HANDLER = 'hdfs.GenericSequenceFileReader';
```

To use the UDTF:

```sql
select * from table(HADOOP_READ_GENERIC('@mystage/part-m-00000'))
```

or 

```sql
select keyParts[0]::string source, keyParts[1]::string date, content contentparts from
(select split(key,',') keyParts, content  from TABLE(HADOOP_READ_GENERIC('@mystage/payload.seq')));
```

After that you can just do an split in snowflake using the right separators.

The Generic SequenceFileReader will also check if the content matches a gz string in which case it will decompress it before returning the content.
> If it fails it will just BASE64 encode the content and return it.
