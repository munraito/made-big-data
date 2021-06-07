#! /usr/bin/env bash
set -x
HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar

hdfs dfs -rm -r $2

yarn jar $HADOOP_STREAMING_JAR \
	-files mapper.py,reducer.py \
	-mapper mapper.py \
	-reducer reducer.py \
	-numReduceTasks 3 \
	-input $1 \
	-output $2

if hdfs dfs -test -e "$2/_SUCCESS"; then
	hdfs dfs -cat $2/* | head -n 50 | tee hw2_mr_data_ids.out
else
	echo "job failed"
fi
