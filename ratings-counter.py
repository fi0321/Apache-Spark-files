from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///spark Course/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
ratings3 = lines.map(lambda x:x)
ratings2=ratings3.collect()
for i in ratings2:
    print(i.)
result = ratings.countByValue()
print (ratings)
print (type(ratings3))

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
