import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

KEYSPACE_NAME = sys.argv[1]

spark = SparkSession \
        .builder \
        .appName("hw9") \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = (
        spark
        .read
        .format('csv')
        .options(delimiter=',', inferSchema=True, header=True)
        .load('hdfs:///data/movielens/movies.csv')
)
df = df.where(col('genres') != '(no genres listed)')
df = df.withColumn('title', trim(regexp_replace(df['title'], '\(\D{4}\)', '')))

df = df.where(col('title').rlike('\(\d{4}\)'))
df = (
    df
    .withColumn('year', regexp_extract(df['title'], '\((\d{4})\)', 1))
    .withColumn('title', regexp_replace(df['title'], '\(\d{4}\)', ''))
)
df = df.withColumn('genres', split(col('genres'), '\|'))
df = df.withColumnRenamed('movieId', 'movieid')

df.write.format("org.apache.spark.sql.cassandra") \
        .options(table="movies", keyspace=KEYSPACE_NAME) \
        .mode("append").save()
