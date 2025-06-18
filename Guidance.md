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
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CustomerSegmentation") \
    .config("spark.mongodb.output.uri", "mongodb+srv://<username>:<password>@<cluster-url>/retail.customers") \
    .config("spark.mongodb.input.uri", "mongodb+srv://<username>:<password>@<cluster-url>/retail.customers") \
    .getOrCreate()
```
**Giải thích**  
➡️ Cấu hình địa chỉ URI để ghi (output) dữ liệu Spark vào MongoDB.
```bash
.config("spark.mongodb.output.uri", "mongodb+srv://<username>:<password>@<cluster-url>/retail.customers") \
```
➡️ Cấu hình địa chỉ URI để đọc (input) dữ liệu từ MongoDB vào Spark.
```bash
.config("spark.mongodb.input.uri", "mongodb+srv://<username>:<password>@<cluster-url>/retail.customers") \
```
➡️ Cú pháp URI MongoDB Atlas:
```bash
mongodb+srv://<username>:<password>@<cluster-url>/<database>.<collection>
```
5. Load and Clean the Dataset
```bash
# Download CSV
!wget -O retail.csv https://raw.githubusercontent.com/christianversloot/machine-learning-articles/main/data/online_retail.csv

df = spark.read.option("header", True).csv("retail.csv")
df.printSchema()

# Filter invalid rows
from pyspark.sql.functions import col

df_clean = df.filter((col("CustomerID").isNotNull()) & (col("Quantity") > 0) & (col("UnitPrice") > 0))
df_clean = df_clean.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
df_clean = df_clean.select("CustomerID", "InvoiceNo", "TotalPrice")

```










   
