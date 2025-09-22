import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id="hudi_minio_ingest",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
  catchup=False,
):
 bash_task = BashOperator(
    task_id="hudi_minio_ingest_sync_hive",
    bash_command="""
    spark-submit \
      --master spark://spark:7077 \
      --deploy-mode client \
      --jars /opt/airflow/external-jars/hudi-spark3.5-bundle_2.12-1.0.2.jar,\
/opt/airflow/external-jars/hadoop-aws-3.3.4.jar,\
/opt/airflow/external-jars/aws-java-sdk-bundle-1.12.262.jar,\
/opt/airflow/external-jars/calcite-core-1.26.0.jar,\
/opt/airflow/external-jars/spark-avro_2.12-3.5.0.jar\
      --driver-class-path /opt/airflow/external-jars/hadoop-aws-3.3.4.jar:/opt/airflow/external-jars/aws-java-sdk-bundle-1.12.262.jar \
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      --conf spark.hadoop.fs.s3a.committer.name=directory \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
      --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
      --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
      --conf spark.speculation=false \
      --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored=true \
      /opt/airflow/jobs/jobs_spark_write_df_sync_hive.py
    """
)