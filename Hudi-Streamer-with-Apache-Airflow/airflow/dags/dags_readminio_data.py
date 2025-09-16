import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id="minio_read_test",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
  catchup=False
):
  BashOperator(
    task_id="minio_read_test_task",
    bash_command="""
spark-submit \
  --master spark://spark:7077 \
  --deploy-mode client \
  --verbose \
  --jars /opt/spark/extra-jars/hudi-spark3.5-bundle_2.12-1.0.2.jar,/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.367.jar \
  --conf 'spark.driver.extraJavaOptions=-Dcom.amazonaws.sdk.disableClockSkewAdjustment=true -Duser.timezone=UTC -Dorg.apache.xerces.features.disallow-doctype-decl=false' \
  --conf 'spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.disableClockSkewAdjustment=true -Duser.timezone=UTC -Dorg.apache.xerces.features.disallow-doctype-decl=false' \
  /opt/airflow/jobs/jobs_readminio.py
""")

# Note: The connection ID (conn_id) used in this DAG must be created through the Airflow interface under Admin > Connections.