import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/home/shafiur/de-zoomcamp-2024/gcp-service-account/service_account.json'

conf = SparkConf() \
	.setMaster('local[*]') \
	.setAppName('test') \
	.set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
	.set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
	.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


sc = SparkContext(conf=conf) 

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


spark = SparkSession.builder \
	.master("local[*]") \
	.appName('test') \
	.config("spark.executor.memory", "4g") \
	.config("spark.driver.memory", "4g") \
	.getOrCreate()

df_fhv = spark.read.parquet('gs://dtc-de-411905-week5/fhv/2019/10/*')


df_fhv.show()

df_fhv.count()
