from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date, rand
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import json
import os
from datetime import datetime
import time
from pyspark.errors.exceptions.captured import AnalysisException

# -------------------- MinIO & Hudi Configurations --------------------
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
    .appName("Hudi Geospatial Write") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------- 1. Dummy Data --------------------
dummy_raw_data = [
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001", "status": "released"}),
     int(datetime(2025, 8, 21, 10, 0, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a5"}, "couponCode": "CPN002", "status": "released"}),
     int(datetime(2025, 8, 21, 11, 30, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a6"}, "couponCode": "CPN003", "status": "released"}),
     int(datetime(2025, 8, 22, 14, 15, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001_UPDATED", "status": "cancelled"}),
     int(datetime(2025, 8, 21, 12, 0, 0).timestamp() * 1000)),
]

raw_schema = StructType([
    StructField("after", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

df_raw = spark.createDataFrame(dummy_raw_data, schema=raw_schema)

# -------------------- 2. Transform for Hudi --------------------
json_schema = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),
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
    'hoodie.datasource.write.operation': 'bulk_insert',
    'hoodie.datasource.hive_sync.enable': 'false', # Keep this off for the test
    'hoodie.metadata.enable': 'false',
    'hoodie.consistency.check.enabled': 'true',
}

# -------------------- 5. Write to Hudi --------------------
print(f"Attempting a simplified bulk_insert to Hudi table '{HUDI_TABLE_NAME}' at '{HUDI_TABLE_PATH}'...")
df_hudi_prep.write.format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite")  \
    .save(HUDI_TABLE_PATH)

print("Simplified write completed!")
spark.stop()
# print("Write completed!")

# # -------------------- 6. Refresh Hive and Verify --------------------
# print("Reading back Hudi table data for verification...")
# hudi_df = spark.read.format("hudi").load(HUDI_TABLE_PATH)
# hudi_df.show()

# print("Reading from Hive table...")

# # Loop with retries is no longer strictly necessary with enableHiveSupport, but it's good practice
# for i in range(5):
#     try:
#         spark.sql(f"REFRESH TABLE default.{HUDI_TABLE_NAME}")
#         hive_df = spark.sql(f"SELECT oid, couponCode, status, ts_ms, ts_date, latitude, longitude FROM default.{HUDI_TABLE_NAME} ORDER BY oid, ts_ms")
#         hive_df.show()
#         break
#     except AnalysisException as e:
#         print(f"Attempt {i+1} failed: Table not found yet. Retrying in 10 seconds...")
#         time.sleep(10)
#         if i == 4:
#             raise e

# spark.stop()