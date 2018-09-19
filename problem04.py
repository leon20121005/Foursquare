from pyspark import SparkConf, SparkContext
import time

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

# struct_time[3] = tm_hour
# UTC (local time) = time.localtime(Unix time)
# Map ['User', 'Venue', 'Time'] into ['Time (hour)', 1] and reduce by key into ['Time (hour)', n]
venue_data = split_data.map(lambda x: (time.localtime(int(x[2]))[3], 1))
reduce_data = venue_data.reduceByKey(lambda a, b: a + b)

# '10:00 - 10:59' = hour_slot(10)
def hour_slot(hour):
    return str(hour) + ':00 - ' + str(hour) + ':59'

# UTC+8
time_zone = 'UTC+' + str(int(-time.timezone / 3600))

# Map into [n, 'Time (hour)'] (arrangement) and print top 24 result
for row in reduce_data.map(lambda a: (a[1], a[0])).top(24):
    print(row[0], hour_slot(row[1]), time_zone)
print('Top 24 result printed...')

# FOR TESTING
# total = 0
# for row in reduce_data.collect():
#     total += int(row[1])
# print (total)
