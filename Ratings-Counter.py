from pyspark import SparkConf, SparkContext
import safeJSON as json
import collections


#To run on EMR successfully + output results for Star Wars:
#aws s3 cp s3://sundog-spark/MovieSimilarities1M.py ./
#aws s3 cp s3://sundog-spark/ml-1m/movies.dat ./
#spark-submit --executor-memory 1g MovieSimilarities1M.py 260

'''
conf = SparkConf()
sc = SparkContext(conf = conf)
'''
def parseLine(line):
	
	key = line[u"data"][u"sslv2"][u"server_hello"][u"certificate"][u"parsed"][u"fingerprint_sha256"] 
	data = line[u"ip"]
	count = 1
	return(key, data, count)


conf = SparkConf().setMaster("local[*]").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#raw = sc.textFile("s3n://testsmallbucket/testsmall.json")
raw = sc.textFile('testsmall.json')
data = raw.map(lambda line: json.loads(line))
datapartion = data.map(parseLine)
#datapartion = data.map(lambda line: (line[u"data"][u"sslv2"][u"server_hello"][u"certificate"][u"parsed"][u"fingerprint_sha256"], 1))
more = datapartion.map(lambda x: (x[0], x[1])).groupByKey().mapValues(list)
morez = more.map(lambda x: (x[0], x[1])).filter(lambda x: len(x[1]) < 2)

#morez = more.groupByKey().mapValues(list)
#mores = morez.map(lambda x: (x[0], x[2]))



#parts = datapartion.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x + y)
#stuff = parts.map(lambda x: (x[0], x[2] x[1]))
certificates = datapartion.map(lambda x: (x[0], x[1]))
#stuff = certificates.groupByKey()

#datas = datapartion.map(lambda z: (z[1]))

#.filter(lambda x: x[0] < 2)
#test_certs = certificates.map(lambda x: (x[0], x[1]))
#filtered_cert = swapped_cert.filter(lambda x: x[0] < 2)


#certificates.saveAsTextFile('file:///Users/cmeffert/Documents/Spark/outputtest')
#datapartion = data.map(lambda line: (line[u"data"][u"sslv2"][u"server_hello"][u"certificate"][u"parsed"][u"fingerprint_sha256"]))
#resultspartioned = datapartion.partitionBy(100)
#results = resultspartioned.countByValue()


results = morez.collect()

for result in results:
	print result


'''
results = reduced.countByValue()
sortedResults = collections.OrderedDict(sorted(results.items()))


for key, value in sortedResults.iteritems():
    print "%s %i" % (key, value)
'''
		

'''
val sc = new SparkContext(...)
val r1 = sc.textFile("xxx1")
val r2 = sc.textFile("xxx2")
...
val rdds = Seq(r1, r2, ...)
val bigRdd = sc.union(rdds)
'''
	

