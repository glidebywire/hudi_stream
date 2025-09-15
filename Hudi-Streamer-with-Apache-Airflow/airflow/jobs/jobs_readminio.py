from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date, lit, create_map, explode
from pyspark.sql.types import StructType, StructField, StringType, MapType, LongType
import json
from datetime import datetime

# MinIO and Hudi configurations (keep as is)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
# MINIO_BUCKET_NAME = "advertising-data-lake" # No longer directly reading from this for initial data
HUDI_BUCKET_NAME = "hudibuckettest"
HUDI_TABLE_NAME = "hudibuckettest" # Using your original Hudi table name
HUDI_TABLE_PATH = f"s3a://{HUDI_BUCKET_NAME}/hive/{HUDI_TABLE_NAME}/"

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
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()

# -------------------- 1. Generate Dummy Data for Hudi ------------------------------
print("Generating dummy data for Hudi table...")

# We need to simulate the structure after your original `df_convers` transformation
# This means providing: 'values' (MapType), 'ts_date' (DateType), 'oid' (StringType), 'ts_ms' (LongType)

# Create raw-like data that, when processed, will result in the structure needed for Hudi
# Simulating the 'after' field which contains JSON, and 'ts_ms'
dummy_raw_data = [
    # Example 1
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001", "status": "released"}),
     int(datetime(2025, 8, 21, 10, 0, 0).timestamp() * 1000)),
    # Example 2
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a5"}, "couponCode": "CPN002", "status": "released"}),
     int(datetime(2025, 8, 21, 11, 30, 0).timestamp() * 1000)),
    # Example 3 (different date for partitioning)
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a6"}, "couponCode": "CPN003", "status": "released"}),
     int(datetime(2025, 8, 22, 14, 15, 0).timestamp() * 1000)),
    # Example 4 (for upsert test - same oid as example 1 but newer timestamp)
    (json.dumps({"_id": {"$oid": "60a7e2b7e9b0c2a0d1e2f3a4"}, "couponCode": "CPN001_UPDATED", "status": "cancelled"}),
     int(datetime(2025, 8, 21, 12, 0, 0).timestamp() * 1000 + 1000)), # +1 sec for higher precombine field
]

# Define schema for raw data
raw_schema = StructType([
    StructField("after", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

# Create a DataFrame from dummy raw data
df_raw = spark.createDataFrame(dummy_raw_data, schema=raw_schema)

# Apply the same transformations as your original df_convers to prepare for Hudi
df_hudi_prep = (df_raw.withColumn("values", from_json(col("after"), MapType(StringType(), StringType())))
                .withColumn("ts_date", to_date(from_unixtime(col("ts_ms") / 1000, "yyyy-MM-dd")))
                .withColumn(
                    "oid",
                    from_json(col("values").getItem("_id"), MapType(StringType(), StringType())).getItem("$oid")
                )
                .drop("after")) # Drop the raw 'after' string, 'values' contains the parsed data

print("Prepared DataFrame schema and sample data for Hudi write:")
df_hudi_prep.printSchema()
df_hudi_prep.show(truncate=False)

# --------------------- 2. Write Data to Hudi Table Minio ------------------------
hudi_options = {
    'hoodie.table.name': HUDI_TABLE_NAME,
    'hoodie.streamer.checkpoint.key': 'kuponku_release_checkpoint', # This might not be strictly needed for a batch overwrite, but good practice
    'hoodie.datasource.write.operation': 'upsert', # Use upsert for subsequent writes if you re-run
    'hoodie.datasource.write.recordkey.field': 'oid',
    'hoodie.datasource.write.partitionpath.field' :'ts_date',
    'hoodie.datasource.write.precombine.field': 'ts_ms',
    'hoodie.datasource.write.table.name': HUDI_TABLE_NAME, # Should match hoodie.table.name
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',

    # Hive Metastore Config (assuming your metastore is running)
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.table': HUDI_TABLE_NAME, # Should match hoodie.table.name
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

print("Write to Hudi table completed successfully!")

# Refresh the Hive Metastore to ensure the table is visible to Spark SQL
print(f"Refreshing Hive table 'default.{HUDI_TABLE_NAME}'...")
spark.sql(f"REFRESH TABLE default.{HUDI_TABLE_NAME}")

# --- (Opsional) Langkah 4: Verifikasi con reading back the data ---
print("Reading data back from Hudi table for verification...")
hudi_df = spark.read.format("hudi").load(HUDI_TABLE_PATH)
hudi_df.show()

# --------------------- Read Data From Hive ------------------------
print(f"Reading data from Hive table 'default.{HUDI_TABLE_NAME}'...")
hive_df = spark.sql(f"SELECT * FROM default.{HUDI_TABLE_NAME}")
hive_df.printSchema()
hive_df.show()

spark.stop()
