from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import from_unixtime, to_date, col

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_NAME = "advertising-data-lake"
# MINIO_PATH = "raw-kuponku/redemption/ts=2025-08-20"
MINIO_PATH = "raw-kuponku/releasing/ts=2025-08-21"
HUDI_BUCKET_NAME = "hudi-bucket-test"
# HUDI_TABLE_NAME = "kuponku-redemption-test"
HUDI_TABLE_NAME = "kuponku-releasing-test"
HUDI_TABLE_PATH = f"s3a://{HUDI_BUCKET_NAME}/{HUDI_TABLE_NAME}/"

spark = SparkSession.builder \
    .appName("Read MinIO JSON") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# -------------------- Read Data From Minio ------------------------------
# df = spark.read.json(f"s3a://{MINIO_BUCKET_NAME}/stream/backblaze_smart/0/000000018347_1753254070718_1753254075335.json")
df = spark.read.format("parquet").load(f"s3a://{MINIO_BUCKET_NAME}/{MINIO_PATH}")
df.show(10)
after_schema = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]))
])
# Konversi kolom after ke struct
df_convers = df.withColumn("values", from_json(col("after"), MapType(StringType(), StringType())))\
            .withColumn("ts_date", to_date(from_unixtime(col("ts_ms") / 1000, "yyyy-MM-dd")))\
            .withColumn(
                "oid",
                from_json(col("values")["_id"], MapType(StringType(), StringType()))["$oid"]
            )\
            .drop("after", "before", "updateDescription", "source")

df_convers.show(10, truncate=False)

# --------------------- Write Data to Hudi Table Minio ------------------------
hudi_options = {
    'hoodie.table.name': HUDI_TABLE_NAME,
    'hoodie.streamer.checkpoint.key': 'kuponku_release_checkpoint',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.recordkey.field': 'oid',
    'hoodie.datasource.write.partitionpath.field' :'ts_date',
    'hoodie.datasource.write.precombine.field': 'ts_ms',
    # hoodie.datasource.write.ordering.field=after._id.$oid
    # 'hoodie.datasource.write.table.name': 'kuponku_redemption_test_1',
    'hoodie.datasource.write.table.name': 'kuponku_releasing_test_1',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    # hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.ComplexKeyGenerator

    # Hive Metastore Config
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': 'default',
    # 'hoodie.datasource.hive_sync.table': 'kuponku_redemption_test_1',
    'hoodie.datasource.hive_sync.table': 'kuponku_releasing_test_1',
    # hoodie.datasource.hive_sync.partition_fields=partition_date
    'hoodie.datasource.hive_sync.mode': 'hms',
    # hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.MultiPartKeysValueExtractor
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083'
}

print(f"Writing data to Hudi table '{HUDI_TABLE_NAME}' at '{HUDI_TABLE_PATH}'...")
df_convers.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(HUDI_TABLE_PATH)

print("Write to Hudi table completed successfully!")

# --- (Opsional) Langkah 4: Verifikasi dengan membaca kembali datanya ---
# print("Reading data back from Hudi table for verification...")
# hudi_df = spark.read.form`at("hudi").load(HUDI_TABLE_PATH)
# hudi_df.show()

spark.stop()