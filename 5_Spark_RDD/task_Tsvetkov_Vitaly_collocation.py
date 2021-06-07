import re
from math import log
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('collocations_top').setMaster('yarn')
sc = SparkContext(conf=conf)

articles = sc.textFile('hdfs:///data/wiki/en_articles_part')
stopwords = sc.textFile('hdfs:///data/stop_words/stop_words_en-xpo6.txt')
stopwords_broadcast = sc.broadcast(stopwords.collect())

content_rdd = (
    articles
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(lambda content: re.findall(r'\w+', content))
    .map(lambda words: [w for w in words if w not in stopwords_broadcast.value])
)
content_rdd.cache()

# words stat
all_words = (
    content_rdd
    .flatMap(lambda x: [(i, 1) for i in x])
)
all_words.cache()
all_words_cnt = all_words.count()
words_proba = (
    all_words
    .reduceByKey(lambda x, y: x + y)
    .map(lambda x: (x[0], x[1] / all_words_cnt))
)
words_proba.cache()

# bigrams stat
all_pairs = (
    content_rdd
    .flatMap(lambda xs: zip(xs, xs[1:]))
    .map(lambda xs: (xs[0] + '_' + xs[1], 1))
)
all_pairs.cache()
all_pairs_cnt = all_pairs.count()
pairs_proba = (
    all_pairs
    .reduceByKey(lambda x, y: x + y)
    .filter(lambda x: x[1] >= 500)
    .map(lambda x: (x[0], x[1] / all_pairs_cnt))
)
pairs_proba.cache()

#NPMI calculation
first_word = (
    pairs_proba
    .map(lambda x: (x[0].split('_')[0], (x[0], x[1])))
    .join(words_proba) # first word P
    .map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))
)
second_word = (
    pairs_proba
    .map(lambda x: (x[0].split('_')[1], (x[0], x[1])))
    .join(words_proba) # second word P
    .map(lambda x: (x[1][0][0], x[1][1]))
)
# second_word.cache()

npmi = (
    # x[1][0][0] - P(ab)
    # x[1][0][1] - P(a)
    # x[1][1] - P(b)
    first_word
    .join(second_word)
    .map(lambda x: (x[0], round(-log(x[1][0][0] / (x[1][0][1] * x[1][1])) / log(x[1][0][0]), 3)))
)

for k, v in npmi.takeOrdered(39, key=lambda x: -x[1]):
    print(k, v, sep='\t')
