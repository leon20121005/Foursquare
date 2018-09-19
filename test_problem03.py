from pyspark import SparkConf, SparkContext

APP_NAME = 'FourSquare'

# Initialize
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = SparkContext(conf = conf)

# Read file
checkingRDD = sc.textFile('checking_local_dedup.txt')
venueRDD = sc.textFile('new_venue_info.txt')

# Data format: [User, Venue, Time]
# Split 'User,Venue,Time User,Venue,Time...' to ['User', 'Venue', 'Time']
checking_split_data = checkingRDD.flatMap(lambda x: x.split(' ')).map(lambda x: x.split(','))
venue_split_data = venueRDD.flatMap(lambda x: x.split('\n')).map(lambda x: x.split(','))

a = checking_split_data.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

# for e in venue_split_data.top(50):
#     print(e)

# for e in a.collect():
#     for d in venue_split_data.collect():
#         if e[0] == d[0]:
#             e[0] = d[1]
#     e[0] = 'none'

# for e in a.top(10):
#     print(e[0], e[1])

# def f(input, venue_split_data):
#     for a in venue_split_data.collect():
#         if input[0] == a[0]:
#             return (a[1], input[1])
#     return ('none', input[1])
#     try:
#         k = venue_split_data.collect()
#     except:
#         return 'ohno'
#     if input == k[0][0]:
#         return 'yes'
#     else:
#         return 'no'

# def f(input, venue_split_data):
#     if len(venue_split_data) != 0:
#         for a in venue_split_data:
#             if input == a[0]:
#                 if a[1] != None and len(a[1]) != 0:
#                     return a[1]
#                 else:
#                     return 'N/A'
#     else:
#         return 'none'
#     return venue_split_data[0][1]

# z = a.map(f)

b = venue_split_data.map(lambda x: (x[0], x[1]))

# z = a.cogroup(venue_split_data).map(lambda x: (x[0], f(x[0], venue_split_data), x[1]))

# cogrouped = a.cogroup(venue_split_data)

# z = cogrouped.mapValues(lambda x: (list(x[0]), list(x[1]))).collect()

c = a.join(b).map(lambda x: (x[1][1], x[1][0])).reduceByKey(lambda a, b: a + b)

for e in c.map(lambda a: (a[1], a[0])).top(20):
    print(e)

# print(z[0][2])

# print(venue_split_data.collect()[0][1])

# print(f('4b0bd124f964a520e03323e3'))

# d = a.join(b)
# for e in z.map(lambda a: (a[1], a[0])).top(20):
#     print(e)
# for e in z.map(lambda a: (a[1], a[0])).top(20):
#     print(e)

# for c in d.collect():
#     print(c)

# df1 = sqlContext.createDataFrame(checking_split_data, ['User', 'Venue', 'Time'])
# df2 = sqlContext.createDataFrame(venue_split_data, ['Venue', 'Category', 'Latitude', 'Longitude'])
# df1.registerTempTable('df1')
# df2.registerTempTable('df2')

# df1.join(df2, df1.Venue == df2.Venue, joinType='inner')
# sqlContext.sql('SELECT * FROM df1 JOIN df2 ON df1.Venue == df2.Venue')

# df1.show()

# Map into ['User', 1] and reduce
# venue_data = split_data.map(lambda x: (x[0], 1))
# reduce_data = venue_data.reduceByKey(lambda a, b: a + b)

# Print top 20 output
# for single in reduce_data.map(lambda a: (a[1], a[0])).top(20):
#     print(single[0], single[1])

# for single in a.map(lambda a: (a[1], a[0])).top(20):
#     print(single[0], single[1])
