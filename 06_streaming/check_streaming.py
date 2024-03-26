#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import*
import time

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"


# In[2]:


#create spark session
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("ThirdConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()


# In[3]:


# Connect to kafka server and read data stream
df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "green-trips") \
.option("startingOffsets", "earliest") \
.load() 
#\
#.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


# In[4]:


df.printSchema()


# In[5]:


from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())


# In[6]:


from pyspark.sql import functions as F

df = df.select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
.select("data.*")


# In[7]:


# Write the input data to memory
query = df.writeStream.outputMode("append").format("memory").queryName("testk2s").option("partition.assignment.strategy", "range").start()

query.awaitTermination(30)

query.stop()

query.status


# In[8]:


# Query data
test_result=spark.sql("select DOLocationID, count(DOLocationID) from testk2s group by DOLocationID order by 2 desc")
test_result.show(5)


# In[9]:


spark.sql("select count(*) from testk2s").show()
test_result_small = spark.sql("select * from testk2s limit 5")
test_result_small.show()

