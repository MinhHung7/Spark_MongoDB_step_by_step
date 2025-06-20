# 📘 Project Guidance: Customer Segmentation with PySpark & MongoDB
This guide will walk you through how to run the CustomerSegmentation project using Apache Spark, PySpark, and MongoDB Atlas in Google Colab.
# 🧱 Requirements
- Google Colab
- MongoDB Atlas cluster (with a collection named retail.customers)
- MongoDB username & password
- Internet connection
# ⚙️ Step-by-Step Setup
1. Setup Spark on Google Colab
```bash
# install java
!apt-get update
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
# install spark (change the version number if needed)
!wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
# unzip the spark file to the current folder
!tar xf spark-3.5.1-bin-hadoop3.tgz
```
2.  Set your spark folder to your system path environment.
```bash
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.1-bin-hadoop3"
```
3. Create Spark Session
```bash
# start pyspark
!pip install findspark
import findspark
findspark.init()
```
4. Start Spark Session
```bash
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local")\
          .appName("Spark APIs Exercises")\
          .config("spark.some.config.option", "some-value")\
          .getOrCreate()
```
5. Read dataset
```bash
linesDF = spark.read.csv("/content/Online Retail.csv")
linesDF.show(5, truncate=False)
```
6. Print Schema
```bash
df = spark.read.option("header", True).csv("/content/Online Retail.csv")
df.printSchema()
```












   
