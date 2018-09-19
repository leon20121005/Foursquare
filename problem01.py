from pyspark import SparkConf, SparkContext

APP_NAME = 'FourSquare'

# Initialize
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = SparkContext(conf = conf)

# Read file
textRDD = sc.textFile('checking_local_dedup.txt')

# Data format: [User, Venue, Time]
# Split 'User,Venue,Time User,Venue,Time...' to ['User,Venue,Time'] and then to ['User', 'Venue', 'Time']
split_data = textRDD.flatMap(lambda x: x.split(' ')).map(lambda x: x.split(','))

# Map ['User', 'Venue', 'Time'] into ['Venue', 1] and reduce by key into ['Venue', n]
venue_data = split_data.map(lambda x: (x[1], 1))
reduce_data = venue_data.reduceByKey(lambda a, b: a + b)

# Map into [n, 'Venue'] (arrangement) and print top 20 result
for row in reduce_data.map(lambda a: (a[1], a[0])).top(20):
    print(row[0], row[1])
print('Top 20 result printed...')
