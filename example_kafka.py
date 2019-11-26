from __future__ import print_function

import sys
import json
import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext

from collections import namedtuple

from pyspark.streaming.kafka import KafkaUtils

# represents a hashtag and its count
TagCount = namedtuple('TagCount', ("tag", "count"))

# extracts the text from the Tweet JSON object
def extractTweetText(tweetJSON):
    if ('text' in tweetJSON):
        return tweetJSON['text']
    else:
        return ''



if __name__ == "__main__":
 #   if len(sys.argv) != 3:
 #       print("Usage: trending_tags.py <hostname> <port>", file=sys.stderr)
 #       exit(-1)
    
    sc = SparkContext(appName = "PythonStreamingRecieverKafkaWordCount")
    windowIntervalSecs = 10
    ssc = StreamingContext(sc, windowIntervalSecs)

    print("*************************************************************************************************")
    kvs = KafkaUtils.createDirectStream(ssc, ["srs"],{"metadata.broker.list": "35.202.223.90:9092"})
    (kvs
 #   .map(lambda tweet: extractTweetText(json.loads(tweet, encoding="utf-8"))) #extract the actual text from each tweet
 #   .flatMap(lambda text: text.split(" ")) #emit all words in tweet
 #   .filter(lambda word: word.startswith('#')) #filter to only hashtag words
 #   .map(lambda word:  (word.lower(), 1)) #emit tuple of (word, 1)
 #   .reduceByKey(lambda a,b: a+b) #count how many time each hashtag occurs
 #   .map(lambda rec: TagCount(rec[0], rec[1] )) #convert into TagCount namedtuples
 #   .foreachRDD(lambda rdd : rdd.toDF().registerTempTable("tag_counts"))
    )

    #Start the collection of data
    ssc.start()
    print("*************************************************************************************************")
    type(kvs)
    print(kvs)
    print("*************************************************************************************************")

    #now we'll peek into our tag_counts table once every 15 secs to see the trending tags
 #   sqlContext = SQLContext(sc)
 #   count = 0
 #   while count < 100:
 #       time.sleep(15)
 #       count = count +1
 #       top_10_hashtags = sqlContext.sql('select tag, count from tag_counts order by count desc limit 10')
 #       for row in top_10_hashtags.collect():
 #           print(row.tag, row['count'])
 #           print('-------------')

    ssc.awaitTermination()