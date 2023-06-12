from pyspark import SparkConf, SparkContext 
conf= SparkConf().setMaster("local").setAppName("FriendsByAge")
sc=SparkContext(conf=conf)

def parseLine(line):
   field=line.split(",")
   name= (field[1])
   numFriends= int(field[3])
   return (name,numFriends)

lines=sc.textFile("file:///spark Course/fakefriends.csv")
rdd=lines.map(parseLine)
totalages=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
averagesByAge=totalages.mapValues(lambda x:x[0]/x[1])
results= averagesByAge.collect()
for result in results:
    print(result)
