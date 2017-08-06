import nltk
from pyspark.sql import SparkSession

# @formatter:off
!hdfs dfs -rm -f -r -skipTrash /tmp/stateunion
!hdfs dfs -mkdir -p /tmp/stateunion/landing
!hdfs dfs -put cloudera-framework-parent/cloudera-framework-example-5/src/test/resources/data/stateunion/nixon/1970/1970-Nixon.txt /tmp/stateunion/landing/1970-Nixon.txt
!hdfs dfs -ls /tmp/stateunion/landing
# @formatter:on

spark = SparkSession.builder \
    .appName("spark-nltk") \
    .getOrCreate()

data = spark.sparkContext.textFile('/tmp/stateunion/landing/1970-Nixon.txt')


def word_tokenize(x):
    return nltk.word_tokenize(x)


def pos_tag(x):
    return nltk.pos_tag([x])


words = data.flatMap(word_tokenize)
words.saveAsTextFile('/tmp/stateunion/processed/tokens')

pos_word = words.map(pos_tag)
pos_word.saveAsTextFile('/tmp/stateunion/processed/tokens_positions')

# @formatter:off
!hdfs dfs -ls /tmp/stateunion/processed
!hdfs dfs -cat /tmp/stateunion/processed/tokens/*
!hdfs dfs -cat /tmp/stateunion/processed/tokens_positions/*
# @formatter:on
