import argparse
from pyspark.sql.functions import col, split, from_unixtime, element_at, when, window, count, approx_count_distinct
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("runet_stat") \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

parser = argparse.ArgumentParser()
parser.add_argument("--kafka-brokers", required=True)
parser.add_argument("--topic-name", required=True)
parser.add_argument("--starting-offsets", default='latest')

group = parser.add_mutually_exclusive_group()
group.add_argument("--processing-time", default='0 seconds')
group.add_argument("--once", action='store_true')

args = parser.parse_args()
if args.once:
    args.processing_time = None
else:
    args.once = None

spark.conf.set("spark.sql.shuffle.partitions", 5)

input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args.kafka_brokers) \
    .option("subscribe", args.topic_name) \
    .option("startingOffsets", args.starting_offsets) \
    .load()

df = input_df.selectExpr("cast(value as string)") \
        .select(from_unixtime(split(col("value"), "\t").getItem(0)).alias("ts"), \
                split(col("value"), "\t").getItem(2).alias("url"), \
                split(col("value"), "\t").getItem(1).alias("uid"))
df.createOrReplaceTempView("page_views")
df = spark.sql("select ts, parse_url(url, 'HOST') as domain, uid from page_views") \
    .select(when(element_at(split(col('domain'), '\\.'), -1) == 'ru', 'ru') \
            .otherwise('not ru').alias('zone'), col('ts'), col('uid'))
res = df.groupBy(
    window('ts', '2 seconds', '1 second'),
    'zone'
).agg(
    count('uid').alias('view'),
    approx_count_distinct('uid').alias('unique')
).sort(
    col('window').asc(),
    col('view').desc(),
    col('zone').asc()
).limit(20)

query = res \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(once=args.once, processingTime=args.processing_time) \
    .start()
#     .option("numRows", 10) \

query.awaitTermination()
