import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id="minio_geospatial_write_test",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
  catchup=False,
):
  BashOperator(
    task_id="minio_geospatial_write_task",
    bash_command="""
chmod +x /opt/airflow/jobs/jobs_readminio.py
spark-submit \
  --master spark://spark:7077 \
  --deploy-mode client \
  --verbose \
  --jars /opt/airflow/external-jars/hudi-spark3.5-bundle_2.12-1.0.2.jar,/opt/airflow/external-jars/hadoop-aws-3.3.4.jar,/opt/airflow/external-jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  /opt/airflow/jobs/jobs_readminio.py
"""
  )