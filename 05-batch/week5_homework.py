#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

parser = argparse.ArgumentParser()

parser.add_argument('--input_loc', required=True)
parser.add_argument('--output_loc', required=True)

args = parser.parse_args()

input_loc = args.input_loc
output_loc = args.output_loc

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
	types.StructField('Affiliated_base_number', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_loc)

df = df.repartition(6)

df.write.parquet(output_loc)
