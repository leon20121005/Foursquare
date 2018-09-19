# from urllib.request import urlopen
# import json

from pyspark import SparkConf, SparkContext
from operator import add
import sys

APP_NAME = 'FourSquare'

# 台北市大安區 座標：25°1′35″N 121°32′4″E
# URL = 'https://api.foursquare.com/v2/venues/search?v=20170530&ll=25.135%2C%20121.324&intent=checkin&oauth_token=4VAWMOSXL541EWZNIFTBEKGG0OI2YOASRKCTIOD1QPAC00CW'
# obj = urlopen(URL)
# data = json.load(obj)

# print(data)

# for venue in data['response']['venues']:
#     print(venue['stats']['checkinsCount'])

# def a(p):
#     b = p.split(',')
#     return b

conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = SparkContext(conf = conf)
textRDD = sc.textFile('checking_local_dedup.txt')
# map_data = textRDD.flatMap(lambda x: x.split(' ')).flatMap(lambda x: x.split(',')).map(lambda x: (x, 1))

d = textRDD.flatMap(lambda x: x.split(' ')).map(lambda x: x.split(','))
e = d.map(lambda x: (x[1], 1))
reduce_data = e.reduceByKey(lambda a, b: a + b)
# reduce_data = reduce_data.top(10, key = lambda x: x[10])

# reduce_data = map_data.reduceByKey(lambda a, b: a + b).collect()
for single in reduce_data.map(lambda a: (a[1], a[0])).top(10):
    print(single[0], single[1])

# from pyspark.sql import SparkSession

# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()

# data_frame = spark.read.json(str(data))
# data_frame.show()
# sc.json(data)
