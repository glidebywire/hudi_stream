# jobs_readminio_fixed.py
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import Row

# ==== Configuration ====
base_bucket = "hudibuckettest"

# ==== Initialize SparkSession safely ====
spark = SparkSession.builder \
    .appName("ReadMinIO") \
    .config("spark.speculation", "false") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

# ==== Parquet smoke test ====
def parquet_smoke_test(spark_session):
    import uuid

    test_uuid = uuid.uuid4()
    unique_test_path = f"s3a://{base_bucket}/hive/_test_parquet_write/test_{test_uuid}/"

    # Correct S3A filesystem
    hadoop_conf = spark_session._jsc.hadoopConfiguration()
    fs_path = spark_session._jvm.org.apache.hadoop.fs.Path(unique_test_path)
    fs = fs_path.getFileSystem(hadoop_conf)

    # Delete path if exists
    if fs.exists(fs_path):
        fs.delete(fs_path, True)

    # Tiny DataFrame
    df = spark_session.createDataFrame([{"id": 1, "value": "ok"}])
    df.write.mode("overwrite").parquet(unique_test_path)

    # Read back
    df_read = spark_session.read.parquet(unique_test_path)
    return df_read.count() == df.count()

# ==== Run smoke test ====
if __name__ == "__main__":
    smoke_ok = parquet_smoke_test(spark)
    print(f"Parquet smoke test successful: {smoke_ok}")
    spark.stop()
