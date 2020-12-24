# Dependencies to setup pyspark on MacOS

* 1. SPARK_HOME (e.g: echo $SPARK_HOME Output: ./my_projects/spark-3.0.1-bin-hadoop2.7)
* 2. PYSPARK_PYTHON --> By default, Python is not part of Spark environment. By setting PYSPARK_PYTHON, establishes relationship between Python and Spark environments. (e.g: echo $PYSPARK_PYTHON. output: ./pyspark)
* 3. PYTHONPATH --> (e.g: echo $PYTHONPATH
/Users/prammitr/Documents/Doc/my_projects/spark-3.0.1-bin-hadoop2.7/python:/Users/prammitr/Documents/Doc/my_projects/spark-3.0.1-bin-hadoop2.7/python/lib/py4j-0.10.9-src.zip)

** Check Setup: This should return valida valie (echo $SPARK_HOME/$python/lib/ Output: /Users/prammitr/Documents/Doc/my_projects/spark-3.0.1-bin-hadoop2.7//lib/)


## Test Local Setup: Run spark-shell (e.g. /Users/my_projects/spark-3.0.1-bin-hadoop2.7/bin/spark-shell)

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/

Using Scala version 2.12.10 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_211)
Type in expressions to have them evaluated.
Type :help for more information.

scala>

## How to quit Scala REPL?

scala> :quit
(base) ✔ ~

## Test Local Setup: Run spark-shell (e.g. /Users/my_projects/spark-3.0.1-bin-hadoop2.7/bin/pyspark)

11:44 $ /Users/my_projects/spark-3.0.1-bin-hadoop2.7/bin/pyspark
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/

Using Python version 2.7.16 (default, Jun  5 2020 22:59:21)
SparkSession available as 'spark'.

