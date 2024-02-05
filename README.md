# Snowpark Hadoop Sequence File Reader

Small snippet to access a Hadoop Sequence File in Snowpark.

It implements an UDTF (User Defined Table Function) that reads a Sequence File and returns its content as rows.

# Build

```
mvn clean package   
```

That will produce a jar in the target directory. For example: `target/sequentialreader-0.0.1-jar-with-dependencies.jar``

# Usage

Upload the jar to your Snowflake account, into a selected stage.
For example if your stage is named `mystage`:

You can register the UDTF like:

```
CREATE OR REPLACE FUNCTION HADOOP_READ(filename String)
  RETURNS TABLE(
customer_no String,
account_no String)
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
