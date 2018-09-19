from pyspark import SparkConf, SparkContext

APP_NAME = 'FourSquare'

# Initialize
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = SparkContext(conf = conf)

# Read file
checkingRDD = sc.textFile('checking_local_dedup.txt')
venueRDD = sc.textFile('new_venue_info.txt') # Using new created file because of the decode error

# Data format: [User, Venue, Time]
# Split 'User,Venue,Time User,Venue,Time...' to ['User,Venue,Time'] and then to ['User', 'Venue', 'Time']
checking_split_data = checkingRDD.flatMap(lambda x: x.split(' ')).map(lambda x: x.split(','))

# Data format: [Venue, Category, Latitude, Longitude]
# Split 'Venue,Category,Latitude,Longitude\nVenue,Category,Latitude,Longitude...' to ['Venue', 'Category', 'Latitude', 'Longitude']
venue_split_data = venueRDD.flatMap(lambda x: x.split('\n')).map(lambda x: x.split(','))

# Map ['User', 'Venue', 'Time'] into ['Venue', 1] and reduce by key into ['Venue', n]
venue_rank = checking_split_data.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

# Map ['Venue', 'Category', 'Latitude', 'Longitude'] into ['Venue', 'Category']
venue_category_table = venue_split_data.map(lambda x: (x[0], x[1]))

# Join ['Venue', n] and ['Venue', 'Category'] into ['Venue', [n, 'Category']] and remove duplicates
joined_table = venue_rank.join(venue_category_table).distinct()

# Map ['Venue', [n, 'Category']] into ['Category', n] and reduce the same key
category_rank = joined_table.map(lambda x: (x[1][1], x[1][0])).reduceByKey(lambda a, b: a + b)

# Map into [n, 'Category'] (arrangement) and print top 20 result
for row in category_rank.map(lambda a: (a[1], a[0])).top(20):
    print(row[0], row[1])
print('Top 20 result printed...')

# FOR TESTING
# x = venue_rank.join(venue_category_table).distinct()
# for row in x.map(lambda a: (a[1], a[0])).top(20):
#     print(row)
