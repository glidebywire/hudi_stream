from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField, StringType, MapType, LongType
import json
import os
from datetime import datetime

# ---------------- MinIO + Hudi Config ----------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

HUDI_BUCKET_NAME = "hudibuckettest"   # physical bucket in MinIO
HUDI_TABLE_NAME = "coupons"           # logical Hudi table name
HUDI_TABLE_PATH = f"s3a://{HUDI_BUCKET_NAME}/hive/{HUDI_TABLE_NAME}/"

os.environ["AWS_JAVA_DISABLE_CLOCK_SKEW_ADJUST"] = "true"
os.environ["AWS_REGION"] = "us-east-1"

# ---------------- Spark Session ----------------
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
    .config("spark.hadoop.fs.s3a.clock.skew.adjust", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "50000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()

# ---------------- Dummy Data ----------------
dummy_raw_data = [
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001", "status": "released"}),
     int(datetime(2025, 8, 21, 10, 0, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a5"}, "couponCode": "CPN002", "status": "released"}),
     int(datetime(2025, 8, 21, 11, 30, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a6"}, "couponCode": "CPN003", "status": "released"}),
     int(datetime(2025, 8, 22, 14, 15, 0).timestamp() * 1000)),
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001_UPDATED", "status": "cancelled"}),
     int(datetime(2025, 8, 21, 12, 0, 0).timestamp() * 1000 + 1000)),
]

raw_schema = StructType([
    StructField("after", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

df_raw = spark.createDataFrame(dummy_raw_data, schema=raw_schema)

df_hudi_prep = (df_raw.withColumn("values", from_json(col("after"), MapType(StringType(), StringType())))
                .withColumn("ts_date", to_date(from_unixtime(col("ts_ms") / 1000, "yyyy-MM-dd")))
                .withColumn("oid",
                            from_json(col("values").getItem("_id"), MapType(StringType(), StringType())).getItem("$oid"))
                .drop("after"))

print("Prepared DataFrame schema:")
df_hudi_prep.printSchema()
df_hudi_prep.show(truncate=False)

# ---------------- Write to Hudi ----------------
hudi_options = {
    'hoodie.table.name': HUDI_TABLE_NAME,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.recordkey.field': 'oid',
    'hoodie.datasource.write.partitionpath.field': 'ts_date',
    'hoodie.datasource.write.precombine.field': 'ts_ms',
    'hoodie.datasource.write.table.name': HUDI_TABLE_NAME,
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.metadata.enable': 'false',
    'hoodie.consistency.check.enabled': 'false',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.table': HUDI_TABLE_NAME,
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083',
    'hoodie.datasource.hive_sync.partition_fields': 'ts_date',
    'hoodie.datasource.hive_sync.recordkey.field': 'oid'
}

print(f"Writing dummy data to Hudi table '{HUDI_TABLE_NAME}' at '{HUDI_TABLE_PATH}'...")
df_hudi_prep.write.format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite") \
    .save(HUDI_TABLE_PATH)

print("Write to Hudi completed!")

# ---------------- Read back ----------------
print("Refreshing Hive table...")
spark.sql(f"REFRESH TABLE default.{HUDI_TABLE_NAME}")

print("Reading from Hudi path...")
hudi_df = spark.read.format("hudi").load(HUDI_TABLE_PATH)
hudi_df.show()

print("Reading from Hive table...")
hive_df = spark.sql(f"SELECT * FROM default.{HUDI_TABLE_NAME}")
hive_df.printSchema()
hive_df.show()

spark.stop()
