
#!/bin/sh

#sudo su

rm -rf medical-dataset

sudo -u hdfs hdfs dfs -rm -r -skipTrash /medical-data-analysis

sudo -u hdfs hdfs dfs -mkdir /medical-data-analysis

sudo -u hdfs hdfs dfs -chmod 0777 /medical-data-analysis

hdfs dfs -copyFromLocal hive.sql /medical-data-analysis

hdfs dfs -copyFromLocal workflow.xml /medical-data-analysis

hdfs dfs -copyFromLocal coordinator.xml /medical-data-analysis

oozie job --oozie http://manager-0:11000/oozie -config coordinator.properties -run

