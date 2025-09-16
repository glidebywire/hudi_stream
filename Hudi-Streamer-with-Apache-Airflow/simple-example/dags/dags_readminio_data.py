import datetime
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
  dag_id="minio_read_test",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
):
  SparkSubmitOperator(
    task_id="minio_read_test_task",
    application="jobs/jobs_readminio.py",
    conn_id="spark_default",
    packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.hudi:hudi-spark3-bundle_2.12:1.0.2",
    verbose=True
  )

# Note: The connection ID (conn_id) used in this DAG must be created through the Airflow interface under Admin > Connections.