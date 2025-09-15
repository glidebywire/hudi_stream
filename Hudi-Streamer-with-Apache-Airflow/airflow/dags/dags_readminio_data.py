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
    packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0,org.apache.hive:hive-jdbc:3.1.2",
    exclude_packages="log4j:log4j,org.slf4j:slf4j-log4j12,org.slf4j:slf4j-api,org.apache.logging.log4j:log4j-slf4j-impl",
    
    verbose=True
  )

# Note: The connection ID (conn_id) used in this DAG must be created through the Airflow interface under Admin > Connections.