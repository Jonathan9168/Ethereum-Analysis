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
        .appName("PartD-gas") \
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

    # -------------------------------------------------------------------------
    # calculating average gas price per month
    # -------------------------------------------------------------------------
    # (month/year,(gas_price,count=1))
    # (month/year,(sum_gas_price,sum_counts))
    # (month/year,avg_gas_price)
    transactions = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv").rdd
    clean_trans = transactions.filter(lambda x: len(x) == 15) \
        .map(lambda x: (time.strftime("%m/%Y", time.gmtime(int(x[11]))), (int(x[9]), 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))

    print(clean_trans.collect())
    # -------------------------------------------------------------------------
    # calculating average gas used per month
    # -------------------------------------------------------------------------
    contr_lines = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv").rdd
    clean_contr = contr_lines.filter(lambda x: len(x) == 6)
    # (to_address,(gas,month/year)) -> transcations
    t_features = transactions.filter(lambda x: len(x) == 15) \
        .map(lambda l: (l[6], (int(l[8]), time.strftime("%m/%Y", time.gmtime(int(l[11]))))))
    # (address,None) -> contracts
    c_features = clean_contr.map(lambda l: (l[0], None))
    # (joined_address,(None,(gas,month/year)))
    joined = c_features.join(t_features)
    # (month/year,(gas,count=1))
    # (month/year,(sum_gas,sum_counts))
    # (month/year,avg_gas)
    joined_rdd = joined.map(lambda l: (l[1][1][1], (int(l[1][1][0]), 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))

    print(joined_rdd.collect())
    # -------------------------------------------------------------------------
    # calculating average gas usage per month of top 10 contracts
    # -------------------------------------------------------------------------
    # addresses of top 10 contracts
    addresses = {"0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444",
                 "0x7727e5113d1d161373623e5f49fd568b4f543a9e",
                 "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
                 "0xfa52274dd61e1643d2205169732f29114bc240b3",
                 "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",
                 "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd",
                 "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3",
                 "0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
                 "0xabbb6bebfa05aa13e908eaa492bd7a8343760477",
                 "0x341e790174e3a4d35b65fdc067b6b5634a61caea"}

    # (to_address,(month/year,gas)) -> transcations
    t_features_1 = transactions.filter(lambda x: len(x) == 15) \
        .map(lambda l: (l[6], (time.strftime("%m/%Y", time.gmtime(int(l[11]))), int(l[8]))))
    # (address,None)
    c_features_1 = clean_contr.map(lambda l: (l[0], None))
    # (joined_address,(None,(month/year,gas)))
    # ('0xf1a6ffbdd175afe163fae880dbcc0a25d383a519', (None, ('05/2017', 45934)))
    joined_1 = c_features_1.join(t_features_1)
    # (month/year,(gas,count=1)) if address is part of top 10
    # (month/year,(sum_gas,sum_counts))
    # (month/year,avg_gas)
    joined_rdd_1 = joined_1.map(lambda l: (l[1][1][0], (int(l[1][1][1]), 1)) if l[0] in addresses else ("other", (1,1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
    
    print(joined_rdd_1.collect())
# -------------------------------------------------------------------------
#     now = datetime.now()  # current date and time
#     date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

#     my_bucket_resource = boto3.resource('s3',
#                                         endpoint_url='http://' + s3_endpoint_url,
#                                         aws_access_key_id=s3_access_key_id,
#                                         aws_secret_access_key=s3_secret_access_key)

# clean_trans.repartition(1).saveAsTextFile("s3a://" + s3_bucket + "/part_d_gas" + date_time + '/avg_monthly_gas_price')
# joined_rdd.repartition(1).saveAsTextFile("s3a://" + s3_bucket + "/part_d_gas" + date_time + '/avg_monthly_gas_used')
spark.stop()
