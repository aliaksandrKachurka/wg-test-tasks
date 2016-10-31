# Test Tasks
Implementation of *data insertion into HBase* located in *hbase* module. *hdfs* one contains *file merging in HDFS* implementation.
## Build
Execute
```
$ mvn clean package
``` 
in the project root.
Executable jars *hdfs-test-task.jar* and *hbase-test-task.jar* will be assembled and placed into the corresponding *target* folders.
## Run
Both atrifacts could be run by `spark-submit`. 
Running *hbase-test-task.jar*:
```
$ spark-submit hbase-test-task.jar
``` 
or directly in local mode:
```
$ java -jar hbase-test-task.jar
``` 
*hdfs-test-task.jar* accepts two parameters: input and output folders:
```
$ spark-submit hdfs-test-task.jar \
hdfs://namenode:8020/path/to/input \
hdfs://namenode:8020/path/to/output
``` 