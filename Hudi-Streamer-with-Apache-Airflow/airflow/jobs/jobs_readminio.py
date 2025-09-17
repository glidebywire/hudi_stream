from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField, StringType, MapType, LongType
import json
import os
from datetime import datetime

# -------------------- MinIO & Hudi Configurations --------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
HUDI_BUCKET_NAME = "hudibuckettest"
HUDI_TABLE_NAME = "coupons"
HUDI_TABLE_PATH = f"s3a://{HUDI_BUCKET_NAME}/hive/{HUDI_TABLE_NAME}/"

os.environ["AWS_JAVA_DISABLE_CLOCK_SKEW_ADJUST"] = "true"
os.environ["AWS_REGION"] = "us-east-1"

# -------------------- Spark Session --------------------
# -------------------- Spark Session --------------------
spark = SparkSession.builder \
    .appName("Hudi Write-Then-Read") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.clock.skew.adjust", "false") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "50000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
    .config("spark.hadoop.fs.s3a.xml.disallow.doctype.decl", "false") \
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.speculation", "false") \
    .getOrCreate()

# -------------------- 1. Dummy Data --------------------
dummy_raw_data = [
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001", "status": "released"}),
     int(datetime(2025, 8, 21, 10, 0, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a5"}, "couponCode": "CPN002", "status": "released"}),
     int(datetime(2025, 8, 21, 11, 30, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a6"}, "couponCode": "CPN003", "status": "released"}),
     int(datetime(2025, 8, 22, 14, 15, 0).timestamp() * 1000)),
    # This is an update, so the operation should be 'upsert' for incremental loads
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001_UPDATED", "status": "cancelled"}),
     int(datetime(2025, 8, 21, 12, 0, 0).timestamp() * 1000)), # removed the +1000 for clarity, precombine handles it
]

raw_schema = StructType([
    StructField("after", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

df_raw = spark.createDataFrame(dummy_raw_data, schema=raw_schema)

# -------------------- 2. Transform for Hudi (Corrected) --------------------

# Define the precise schema of the JSON data in the 'after' column
json_schema = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),
    StructField("couponCode", StringType(), True),
    StructField("status", StringType(), True)
])

# Parse the JSON with the correct schema and flatten it
df_parsed = df_raw.withColumn("parsed_after", from_json(col("after"), json_schema))

df_hudi_prep = df_parsed.select(
    col("parsed_after._id.$oid").alias("oid"), # This is our record key
    col("parsed_after.couponCode"),
    col("parsed_after.status"),
    col("ts_ms"), # This is our precombine key
    to_date(from_unixtime(col("ts_ms") / 1000)).alias("ts_date") # This is our partition path
)

print("Correctly Prepared DataFrame for Hudi:")
df_hudi_prep.printSchema()
df_hudi_prep.show(truncate=False)

# -------------------- 3. Hudi Write Options --------------------
hudi_options = {
    'hoodie.table.name': HUDI_TABLE_NAME,
    'hoodie.datasource.write.recordkey.field': 'oid',
    'hoodie.datasource.write.partitionpath.field': 'ts_date',
    'hoodie.datasource.write.precombine.field': 'ts_ms',
    'hoodie.datasource.write.table.name': HUDI_TABLE_NAME,
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    # --- BEST PRACTICE: Use 'upsert' for data with updates ---
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': 3,
    # --- BEST PRACTICE: Enable metadata for better performance on larger tables ---
    'hoodie.metadata.enable': 'false',
    'hoodie.consistency.check.enabled': 'true',
    # Hive Sync
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.table': HUDI_TABLE_NAME,
    'hoodie.datasource.hive_sync.mode': 'jdbc',
    'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:postgresql://metastore-db:5432/metastore_db',
    'hoodie.datasource.hive_sync.username': 'hive',
    'hoodie.datasource.hive_sync.password': 'hive',
    'hoodie.datasource.hive_sync.partition_fields': 'ts_date',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
}


# -------------------- 5. Write to Hudi --------------------
spark.sql("CREATE DATABASE IF NOT EXISTS default")
print(f"Writing data to Hudi table '{HUDI_TABLE_NAME}' at '{HUDI_TABLE_PATH}'...")
df_hudi_prep.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(HUDI_TABLE_PATH)

print("Write completed!")

# -------------------- 6. Refresh Hive and Verify --------------------
# A direct read from the path is sufficient for verification
print("Reading back Hudi table data for verification...")
hudi_df = spark.read.format("hudi").load(HUDI_TABLE_PATH)
hudi_df.show()

import time
from pyspark.errors.exceptions.captured import AnalysisException

# If Hive sync is enabled, you can also query via Spark SQL
print("Reading from Hive table...")

for i in range(5):
    try:
        spark.sql(f"REFRESH TABLE default.{HUDI_TABLE_NAME}")
        hive_df = spark.sql(f"SELECT oid, couponCode, status, ts_ms, ts_date FROM default.{HUDI_TABLE_NAME} ORDER BY oid, ts_ms")
        hive_df.show()
        break
    except AnalysisException as e:
        print(f"Attempt {i+1} failed: Table not found yet. Retrying in 10 seconds...")
        time.sleep(10)
        if i == 4:
            raise e

spark.stop()