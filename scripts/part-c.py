import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PartC") \
        .getOrCreate()

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    blocks = spark.read.option("header", True).csv(
        "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv").rdd

    # (miner,size)
    clean_blocks = blocks.filter(lambda x: len(x) == 19) \
        .map(lambda l: (l[9], int(l[12]))) \
        .reduceByKey(lambda x, y: x + y)

    top_10 = clean_blocks.takeOrdered(10, key=lambda x: -(x[1]))

    for i, record in enumerate(top_10):
        print(f"RANK:{i + 1},MINER:{record[0]},SIZE OF BLOCKS MINED:{record[1]}")

    spark.stop()
