# Dependencies to setup pyspark on MacOS

* 1. SPARK_HOME (e.g: echo $SPARK_HOME Output: ./my_projects/spark-3.0.1-bin-hadoop2.7)
* 2. PYSPARK_PYTHON --> By default, Python is not part of Spark environment. By setting PYSPARK_PYTHON, establishes relationship between Python and Spark environments. (e.g: echo $PYSPARK_PYTHON. output: ./pyspark)
* 3. PYTHONPATH --> 
