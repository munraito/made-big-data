from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# define spark app name
spark = SparkSession.builder.appName("twitter_BFS").getOrCreate()

# define constants
SOURCE = 12
TARGET = 34

# load and preprocess dataset
df = (
        spark
        .read
        .format('csv')
        .options(delimiter='\t', inferSchema=True)
        .load('hdfs:///data/twitter/twitter.txt')
)
df = df.withColumnRenamed("_c0", "user").withColumnRenamed("_c1", "follower")

curr = df.filter(col("follower") == SOURCE).select(col("user"))
cnt = 0
while True:
    cnt += 1
    curr.cache()
    curr = curr.withColumnRenamed('user', 'follower')
    if len(curr.filter(col("follower") == TARGET).head(1)) > 0:
        break
    curr = curr.join(df, 'follower', 'inner').select(col('user'))

print(cnt)
