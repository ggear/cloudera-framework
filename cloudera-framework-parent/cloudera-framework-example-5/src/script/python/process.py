import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
         .appName("spark-nltk") \
         .getOrCreate()
 
data = spark.sparkContext.textFile('/tmp/stateunion/1970-Nixon.txt')
 
def word_tokenize(x):
   import nltk
   return nltk.word_tokenize(x)
 
def pos_tag(x):
   import nltk
   return nltk.pos_tag([x])
 
words = data.flatMap(word_tokenize)
words.saveAsTextFile('/tmp/stateunion/nixon_tokens')
 
pos_word = words.map(pos_tag)
pos_word.saveAsTextFile('/tmp/stateunion/nixon_token_pos')
