#!/usr/bin/env python3
# jobs_readminio_safe.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date, rand
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime
import json
import os
import traceback

# -------------------- MinIO & Hudi Config --------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
HUDI_BUCKET_NAME = "hudibuckettest"
HUDI_TABLE_NAME = "coupons_geo"
HUDI_TABLE_PATH = f"s3a://{HUDI_BUCKET_NAME}/hive/{HUDI_TABLE_NAME}/"

os.environ["AWS_JAVA_DISABLE_CLOCK_SKEW_ADJUST"] = "true"
os.environ["AWS_REGION"] = "us-east-1"

# -------------------- Spark Session --------------------
spark = SparkSession.builder \
    .appName("Hudi Geospatial Write Safe") \
    .master("local[2]")  \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.speculation", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------- Hadoop / S3A configs --------------------
hconf = spark.sparkContext._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hconf.set("fs.s3a.fast.upload", "true")
hconf.set("fs.s3a.committer.name", "magic")
hconf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
hconf.set("fs.s3a.disable.xml.external.entities", "true")  # <<-- Prevent SAXParseException
hconf.set("fs.s3a.connection.timeout", "120000")
hconf.set("fs.s3a.connection.establish.timeout", "120000")
hconf.set("fs.s3a.attempts.maximum", "20")
hconf.set("fs.s3a.connection.proxy.host", "")
hconf.set("fs.s3a.connection.proxy.port", "")

print("MinIO Hadoop S3A configuration set successfully.")

# -------------------- Dummy Data --------------------
dummy_raw_data = [
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001", "status": "released"}), int(datetime(2025, 8, 21, 10, 0, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a5"}, "couponCode": "CPN002", "status": "released"}), int(datetime(2025, 8, 21, 11, 30, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a6"}, "couponCode": "CPN003", "status": "released"}), int(datetime(2025, 8, 22, 14, 15, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001_UPDATED", "status": "cancelled"}), int(datetime(2025, 8, 21, 12, 0, 0).timestamp() * 1000)),
]

raw_schema = StructType([
    StructField("after", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

df_raw = spark.createDataFrame(dummy_raw_data, schema=raw_schema)

# -------------------- Transform for Hudi --------------------
json_schema = StructType([
    StructField("_id", StructType([StructField("$oid", StringType(), True)]), True),
    StructField("couponCode", StringType(), True),
    StructField("status", StringType(), True)
])

df_parsed = df_raw.withColumn("parsed_after", from_json(col("after"), json_schema))

df_hudi_prep = df_parsed.select(
    col("parsed_after._id.$oid").alias("oid"),
    col("parsed_after.couponCode"),
    col("parsed_after.status"),
    col("ts_ms"),
    to_date(from_unixtime(col("ts_ms") / 1000)).alias("ts_date")
).withColumn("latitude", (rand() * 180) - 90) \
 .withColumn("longitude", (rand() * 360) - 180)

print("Prepared DataFrame for Hudi:")
df_hudi_prep.printSchema()
df_hudi_prep.show(truncate=False)

# -------------------- Hudi Write Options --------------------
hudi_options = {
    'hoodie.table.name': HUDI_TABLE_NAME,
    'hoodie.datasource.write.recordkey.field': 'oid',
    'hoodie.datasource.write.partitionpath.field': 'ts_date',
    'hoodie.datasource.write.precombine.field': 'ts_ms',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.metadata.enable': 'true',
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': '3',
    'hoodie.datasource.hive_sync.enable': 'false',
    'hoodie.parquet.max.file.size': 134217728,
    'hoodie.copyonwrite.record.size.estimate': 1024,
    'hoodie.embed.timeline.server': 'false',
    'hoodie.datasource.write.insert.shuffle.parallelism': 2,
    'hoodie.datasource.write.upsert.shuffle.parallelism': 2,
}

# -------------------- Write to Hudi --------------------
try:
    print(f"Attempting upsert to Hudi table '{HUDI_TABLE_NAME}' at '{HUDI_TABLE_PATH}'...")
    df_hudi_prep.write.format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(HUDI_TABLE_PATH)
    print("Hudi upsert completed successfully!")
except Exception:
    print("Hudi write FAILED. Full exception:")
    traceback.print_exc()
finally:
    spark.stop()
