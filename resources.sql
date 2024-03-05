

put file:///Users/mrojas/sf-wls-mapredule-dish/hadoop_file_reader/target/sequentialreader-0.0.1-jar-with-dependencies.jar @mystage
auto_compress=False overwrite=True;


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