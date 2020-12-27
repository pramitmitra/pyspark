# pyspark Dependencies to setup on MacOS

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

## Setup Intellij IDEA for Scala as Spark programming language and SBT as build tool.
### Steps:
* 1. Install IntelliJ (Community or otherwise)
* 2. Start IntelliJ
* 3. Scala Plugin Install: Click IntelliJ -> Congigure -> Plugin -> Search For "Scala" -> Install -> Restart IDE.
* 4. IntelliJ -> Open

### Sample Code: github.com/LearningJournal/Spark-Streaming-in-Scala

## Learning Notes:: (If see error like ->Error: Could not find or load main class )
Then make sure to import porject from external source in Intellij and import as SBT project.
Steps: File>New> Project from Existing Source>(select) Import project from external model> (select)sbt click Next>Finish


# Kafka Instalation
https://kafka.apache.org/quickstart
## 1. Download Kafka binary
### 1.1. zookeeper.properties -> Used by Zookeeper Server
* Changes --> dataDir=../kafka-logs/zookeeper
### 1.2. server.properties -> Kafka broker
* Changes --> Uncomment --> listeners=PLAINTEXT://:9092
Update --> log.dirs=../kafka-logs/server-0

## 2 Set KAFKA_HOME & KAFKA/BIN to source path
### 2.1 Start ZooKeeper --> bin/zookeeper-server-start.sh config/zookeeper.properties
### 2.2 Start Kafka Server --> bin/kafka-server-start.sh config/server.properties
### 2.3 Create "topic" --> bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092 -->Created topicquickstart-events.
### 2.4 Kafka Producer --> kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
>Hello Spark Streaming1
>Hello Spark Streaming2

### 2.5 Kafka Consumer --> kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
>Hello Spark Streaming1
>Hello Spark Streaming2


### Netcat in MACOS
> nc -l 9999



