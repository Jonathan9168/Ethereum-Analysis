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
        .appName("PartD-overhead") \
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
    
    
    def classify_contracts(contract):
        if contract[3] == "False" and contract[4] == "False":
            return ("NONE",1)
        elif contract[3] == "True" and contract[4] == "False":
            return ("ERC20",1)
        elif contract[3] == "False" and contract[4] == "True":
            return ("ERC721",1)
        else:
            return ("BOTH",1)
        
    contracts = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv").rdd    
    print(contracts.map(classify_contracts).reduceByKey(lambda x,y: x+y).collect())
    spark.stop()
