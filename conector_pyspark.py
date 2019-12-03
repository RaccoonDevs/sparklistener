from __future__ import print_function

import sys
import json
import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from collections import namedtuple
from pyspark.streaming.kafka import KafkaUtils, OffsetRange


offsetRanges = []

def storeOffsetRanges(rdd):
     global offsetRanges
     offsetRanges = rdd.offsetRanges()
     return rdd

def printOffsetRanges(rdd):
     for o in offsetRanges:
         print("Leyendo topico {} en la particion {} desde offset {} hasta offset {}".format(o.topic, o.partition, o.fromOffset, o.untilOffset))

def enterDiagnosis(diagnosis):
    for dx in diagnosis:
        return dx['key_diagnosis']

TagCount = namedtuple('TagCount', ("tag", "count"))

sc = SparkContext(appName = "PythonStreamingRecieverKafkaWordCount")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 10)

#KafkaUtils.createStream(ssc, '35.202.223.90:2181', 'spark-streaming', {'srs':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['srs'], kafkaParams = {"metadata.broker.list": '35.202.223.90:9092'})

kafkaStream \
    .transform(storeOffsetRanges) \
    .foreachRDD(printOffsetRanges)

kafkaStream.count().pprint()

kafkaStream.map(lambda tuple:  json.loads( tuple[1]))\
    .map(lambda x: x['clue']) \
    .map(lambda x: (x,1)) \
    .reduceByKey(lambda x, y : x + y)\
    .map(lambda x: TagCount( x[0], x[1])).pprint()
#    .foreachRDD(lambda x: x.toDF().registerTempTable("clue_counts"))


#    .map(lambda json: json['diagnosis'] )
#    .map(lambda diagnosis: enterDiagnosis(diagnosis)).pprint()

#kafkaStream = kafkaStream.map(lambda diagnosis:  (diagnosis, 1))

#kafkaStream = kafkaStream.reduceByKey(lambda x,y: x+y)




#(kafkaStream
#.map(lambda x: json.loads(x))
#.flatMap(lambda tweet: tweet.split(" "))
#.filter(lambda word: '#' in word)
#.map(lambda word:  (word.lower(), 1)) #emit tuple of (word, 1)
#.reduceByKey(lambda a,b: a+b) #count how many time each hashtag occurs
#.map(lambda rec: TagCount(rec[0], rec[1] )) #convert into TagCount namedtuples
#.foreachRDD(lambda rdd : rdd.toDF().registerTempTable("tag_counts"))
#)

#kafkaStream.count().map(lambda x:'Rows in this batch: %s' % x).pprint()


ssc.start()


#sqlContext = SQLContext(sc)
#count = 0
#
#while count < 100:
#    time.sleep(10)
#    count = count +1
#    top_clues = sqlContext.sql('select tag, count from tag_counts order by count desc')
#    for row in top_clues.collect():
#        print(row.tag, row['count'])
#        print('-------------')






ssc.awaitTermination(timeout=180)