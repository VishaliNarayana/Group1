# Group1
README FILE FOR DATA CLEANING PROJECT PHASE3
CLI method for phase 2 is updated
Username, Path and type are passed as arguments to DataUsageTracker from hdfs.h
(path for hdfs.h in git: Group1/hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/hdfs.h)
ResourceManager.java path in git:  Group1/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceManager.java
hdfs.h calls DataUsageTracker class. As bash file calls a java class main method is referred. So, arguments are retrieved in main method and are passed to trackDataUsage method.
main method in DatausageTracker is to pass parameters to other methods from ResourceManager.java or hdfs.h
ProvenanceTracker---------
In ProvenanceTracker class input is taken from user about UsageInfo and respective results are to be displayed.
schemaExists() method connects to hive and verifies for existance of UsageInfo hive database.
createSchema() method connects to hive and creates a database if above method results to non existing database.
tableExists() method connects to hive to verify existance of external hive table in given schema.
If exists data is loaded(overwritten everytime) to table from UsageInfo log file of hdfs path which is updated whenever user accesses data sets.
createTable() method connects to hive and creates a table in respective database if former method suggests no such table found.
Entire UsageInfo log file from hdfs is loaded into this table for UsageInfo.
getProvenanceInformation() method calls all the above methods to gaurantee the retrieval of information from UsageInfo.log to hive UsageInfo table.
This method accepts any combination of arguments(user,path,type,timestamp) from user for information of other attributes.
Later queries the input from hive and returns the respective results to main method.
Provenancetracker.sh and Main method options passed from and to main method are to be handled in phase IV.
