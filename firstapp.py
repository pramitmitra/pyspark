from pyspark import SparkContext
logFile = "file:///Users/prammitr/Documents/Doc/my_projects/pySpark/doc/copyright"  
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print (numAs)
print (numBs)
#print ("Lines with a-> numAs:%d, lines with b-> numBs:%d") % numAs, numBs
print ("Lines with a-> numAs:%d") % (numAs)