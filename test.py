import pyspark
sc = pyspark.SparkContext('local[*]')

txt = sc.textFile('file:////Users/prammitr/Documents/Doc/my_projects/pySpark/doc/copyright')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())
