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
        .appName("PartB") \
        .getOrCreate()


    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            float(fields[7])
            return True
        except:
            return False


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

    trans_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contr_lines = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv").rdd
    trans_clean_lines = trans_lines.filter(good_line)
    contr_clean_lines = contr_lines.filter(lambda x: len(x) == 6)
    
    # (to_address,value)
    t_features = trans_clean_lines.map(lambda l: (l.split(','))).map(lambda l: (l[6], int(l[7])))
    # (address,None)
    c_features = contr_clean_lines.map(lambda l: (l[0], None))

    # (joined_address,(None,value))
    joined = c_features.join(t_features)

    # (joined_address,value)
    joined_rdd = joined.map(lambda l: (l[0], l[1][1])) \
        .reduceByKey(lambda x, y: x + y)

    top_10 = joined_rdd.takeOrdered(10, key=lambda x: -(x[1]))

    for i, record in enumerate(top_10):
        print(f"RANK:{i + 1},ADDRESS:{record[0]},VALUE:{record[1]}")

    spark.stop()
