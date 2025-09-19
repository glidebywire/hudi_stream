import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id="minio_read_test",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
  catchup=False,
):
  BashOperator(
  task_id="minio_read_test_task",
  bash_command="""
chmod +x /opt/airflow/jobs/jobs_readminio.py
spark-submit \
  --master spark://spark:7077 \
  --deploy-mode client \
  --verbose \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000' \
  --conf 'spark.hadoop.fs.s3a.access.key=minioadmin' \
  --conf 'spark.hadoop.fs.s3a.secret.key=minioadmin' \
  --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
  /opt/airflow/jobs/jobs_readminio.py
"""
)