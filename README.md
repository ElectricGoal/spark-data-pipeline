# spark-data-pipeline

(In developing)

Setting up and building an abstract spark data pipeline with Hadoop, Hive, Spark

### Set up

The step that takes the most time (in my case 90%)

There are several tutorials from Internet for these types of installations, but I will list some website that I relied on and had success with Ubuntu 22.04:
- Scala 2.12: https://www.scala-lang.org/download 
- Hadoop 3.5: https://www.howtoforge.com/how-to-install-apache-hadoop-on-ubuntu-22-04
- Spark 3.5.0: when you install Hadoop and run successfully, this step may easy
- Hive 3.1.3: I have to combine many tutorial source to get my Hive running completely. However, I think this is the critical source that I almost consulted: https://sparkbyexamples.com/apache-hive/apache-hive-installation-on-hadoop

### Running

Main commands to run this pipeline. Maybe some blockers due to your environment set up (most common is files, folders authentication between users in same OS)

**Start Hadoop services**
```
start-dfs.sh
start-yarn.sh
```

**Start Hive services**
```
hive --service hiveserver2
hive --service metastore
```

**Compile and run pipeline**

```
cd spark-data-pipeline

sbt package

# my additional step to move output file to shared folder is authenticated for 2 users (one for main user and one for hadoop environment user)
sudo cp ./target/scala-2.12/spark-etl_2.12-1.0.jar /home/sharedFoler/spark-etl.jar  
```

```
# Ingest data from source to hdfs
spark-submit --class Ingestion /home/sharedFoler/spark-etl.jar --sourceFd /home/sharedFoler/data/links.csv --saveFd links --executionDate 2023-10-08

# Transform data and load to Hive
spark-submit --class Transformation /home/sharedFoler/spark-etl.jar --sourceFd links --tableName test.links --executionDate 2023-10-08

```

Using beeline in Hive to query data
```
beeline -u jdbc:hive2://127.0.0.1:10000 
```