import re
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('bigrams_narodnaya').setMaster('yarn')
sc = SparkContext(conf=conf)

articles = sc.textFile('hdfs:///data/wiki/en_articles_part')

words_rdd = (
    articles
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(lambda content: re.findall(r'\w+', content))
    .flatMap(lambda xs: zip(xs, xs[1:]))
    .filter(lambda xs: xs[0] == 'narodnaya')
    .map(lambda xs: (xs[0] + '_' + xs[1], 1))
    .reduceByKey(lambda x, y: x + y)
    .sortByKey(ascending=True)
)
for k, v in words_rdd.collect():
    print(k, v, sep='\t')
