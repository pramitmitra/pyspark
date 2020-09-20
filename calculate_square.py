import pyspark
from pyspark import SparkContext
sc =SparkContext()

nums= sc.parallelize([1,2,3,4,5,6,7,8,9])	

nums.take(1) 
squared = nums.map(lambda x: x*x).collect()
for num in squared:
    print('%i ' % (num))