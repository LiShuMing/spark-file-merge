#! /bin/bash

#export MERGER_INCLUDE_LAST_MONTH=1
#export MERGER_NUM_THREAD=30

# CONNECT 'jdbc:derby:/home/da/lchen/mergetool/derbydb;';
# CONNECT 'jdbc:derby:/home/da/lchen/mergetool/derbydb;create=true';
# CREATE TABLE PARTITION  (dir VARCHAR(32672) PRIMARY KEY, numMerged INTEGER, inputPaths CLOB);


export SPARK_HOME=/home/da/spark
export HADOOP_HOME=/home/da/hadoop
export JAVA_HOME=/home/da/opt/jdk1.7.0_75/

WORK_PATH=/home/da/lchen/mergetool

#HIVE_DATA_PATH=hdfs://hz-cluster1/user/da/uda_hive_warehouse/uda.db/uda_events
# activenamenodeIP:port
#HIVE_DATA_PATH=hdfs://dap1.lt.163.org:8020/user/da/uda/uda_events_new
HIVE_DATA_PATH=hdfs://hz-appops-cluster1/user/da/uda/uda_events_new

yesterday=`date "+%Y-%m-%d" -d "-1days"`

# print log
print(){
#        echo "$@"
        echo "$@" >> $WORK_PATH/run.log
}

CLASSPATH=""

for jar in $WORK_PATH/lib/*
do
    if [ -z $CLASSPATH ]
    then
        CLASSPATH=$jar
    else
        CLASSPATH=$CLASSPATH,$jar
    fi
done

submitMergeTask(){
	print "start to merge: $1"
	$SPARK_HOME/bin/spark-submit \
			--master yarn-client \
			--num-executors 10 \
			--executor-cores 4 \
			--executor-memory 10g  \
			--driver-memory 5g  \
			--jars $CLASSPATH \
			--class netease.bigdata.mergetool.Main \
			$WORK_PATH/parquet-mergetool-0.4-SNAPSHOT-hubble.jar $1
}

# start
print "======= `date "+%Y-%m-%d %H:%M:%S"` daily merge task start ======================"

# get all product data and sumit merge task
dataPath=""
for i in `seq 10`
do
	day=`date "+%Y-%m-%d" -d "-$i days"`
	products=`$HADOOP_HOME/bin/hdfs dfs -ls $HIVE_DATA_PATH/productid=*/*/* | grep "day=$day" | awk '{print $8}'`
	for p in $products
	do
#        echo $p
		if [ -z $dataPath ]
		then
			dataPath=$p
		else
			dataPath=$dataPath,$p
		fi
	done
done

#echo $dataPath

submitMergeTask $dataPath
#submitMergeTask "hdfs://dap1.lt.163.org:8020/user/da/uda/uda_events_test/productid=1000/appid=MA-A4FE-A88932E7A98F/day=2016-12-23"

# finish
print "======= `date "+%Y-%m-%d %H:%M:%S"` daily merge task finished ======================"



