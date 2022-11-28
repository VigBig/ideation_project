'''

@Author: Vighnesh Harish Bilgi
@Date: 2022-11-25
@Last Modified by: Vighnesh Harish Bilgi
@Last Modified time: 2022-11-25
@Title : Ideation Project - 2. Run this program in EMR tp read .csv file from S3 bucket and perform word count and output the result to S3 Bucket

'''

import findspark 
findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


S3_DATA_SOURCE_PATH = 's3://dataset-input-bucket/data-source/dataset.csv'
S3_DATA_OUTPUT_PATH = 's3://dataset-input-bucket/data-output'
S3_BODY_OUTPUT_PATH = 's3://dataset-input-bucket/data-body-output'
S3_TITLE_OUTPUT_PATH = 's3://dataset-input-bucket/data-title-output'

spark = SparkSession.builder.master("local").appName("Spark : Word Count").getOrCreate()
sc = spark.sparkContext
print(sc.appName)


df = spark.read.format('csv').options(header='true', inferSchema='true').load(S3_DATA_SOURCE_PATH)

# df = spark.read.option("header",True).csv("dataset.csv")

df = df.withColumn('title_word_count', f.size(f.split(f.col('title'), ' ')))
df = df.withColumn('body_word_count', f.size(f.split(f.col('body'), ' ')))
df.printSchema()
df.show()

df.coalesce(1).write.option("header",True).csv(S3_DATA_OUTPUT_PATH)
# df.write.option("header",True).csv("updated_dataset")

body_df = df.withColumn('body_word', f.explode(f.split(f.col('body'), ' '))).groupBy('body_word').count().sort('count', ascending=False)
body_df.printSchema()
body_df.show()

body_df.coalesce(1).write.option("header",True).csv(S3_BODY_OUTPUT_PATH)
# body_df.write.option("header",True).csv("body_word_count")

title_df = df.withColumn('title_word', f.explode(f.split(f.col('title'), ' '))).groupBy('title_word').count().sort('count', ascending=False)
title_df.printSchema()
title_df.show()

title_df.coalesce(1).write.option("header",True).csv(S3_TITLE_OUTPUT_PATH)
# title_df.write.option("header",True).csv("title_word_count")