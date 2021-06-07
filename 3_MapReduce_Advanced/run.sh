#! /usr/bin/env bash
set -x
HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar

hdfs dfs -rm -r -skipTrash $2

(yarn jar $HADOOP_STREAMING_JAR \
	-D mapreduce.job.name="Streaming StackOverflow TagCount. Phase 1" \
	-D stream.num.map.output.key.fields=2 \
	-D stream.num.reduce.output.key.fields=2 \
	-files mapper.py,reducer.py \
	-mapper mapper.py \
	-combiner reducer.py \
	-reducer reducer.py \
	-numReduceTasks 8 \
	-input $1 \
	-output $2_tmp &&

yarn jar $HADOOP_STREAMING_JAR \
	-D mapreduce.job.name="Streaming StackOverflow TagCount. Phase 2" \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
	-D mapreduce.partition.keycomparator.options="-k1,1n -k2,2nr" \
	-files second_mapper.py,second_reducer.py \
	-mapper second_mapper.py \
	-reducer second_reducer.py \
	-numReduceTasks 1 \
	-input $2_tmp \
	-output $2
) || echo "Error happens"

#	-D stream.num.reduce.output.key.fields=2 \
#	-combiner reducer.py \

#hdfs dfs -tail $2_temp/*
hdfs dfs -rm -r -skipTrash $2_tmp

if hdfs dfs -test -e "$2/_SUCCESS"; then
	hdfs dfs -cat $2/* | tee hw3_mr_advanced_output.out
else
	echo "job failed"
fi
