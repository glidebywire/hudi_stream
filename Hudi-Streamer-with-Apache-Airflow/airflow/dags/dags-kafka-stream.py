# Lokasi file: dags/ingest_kafka_to_hudi_dag.py
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

PROPERTIES_FILE_PATH = "config/kafka-stream.properties"

with DAG(   
    dag_id="deltastreamer_kuponku_redeem",
    start_date=datetime(2025, 8, 8),
    schedule="@once", # wiwi can set timedelta(minutes=2) too
    catchup=False,
    tags=["hudi", "kafka", "delasteamer", "kuponku", "redemption"],
) as dag:
    ingest_task = SparkSubmitOperator(  
        task_id="deltastreamer_kuponku_redeem_task",
        deploy_mode='cluster',
        application='/opt/spark/extra-jars/hudi-spark3.5-bundle_2.12-1.0.2.jar',
        conn_id="spark_default",
        java_class="org.apache.hudi.utilities.streamer.HoodieStreamer",
        conf={
            'spark.master': 'spark://spark:7077',
            'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6',
            # proxy setting
            'spark.driver.extraJavaOptions': '-Dhttp.proxyHost=10.31.255.65 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.31.255.65 -Dhttps.proxyPort=8080 -Duser.timezone=UTC',
            'spark.executor.extraJavaOptions': '-Dhttp.proxyHost=10.31.255.65 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.31.255.65 -Dhttps.proxyPort=8080 -Duser.timezone=UTC',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',

            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.sql.extensions': 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.hudi.catalog.HoodieCatalog',
            'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
            'spark.sql.hive.convertMetastoreParquet': 'false',
            'spark.hadoop.fs.s3a.clock.skew.adjust.enable': 'false',
            'spark.executor.cores': '1',
            'spark.cores.max': '1'
        },
        application_args=[
            '--props', PROPERTIES_FILE_PATH,
            '--target-base-path', 's3a://advertising-data-lake/hudi-kuponku',
            '--target-table', 'kuponku_redemption',
            '--table-type', 'COPY_ON_WRITE',
            '--source-class', 'org.apache.hudi.utilities.sources.AvroKafkaSource',
            '--schemaprovider-class', 'org.apache.hudi.utilities.schema.SchemaRegistryProvider',
            '--op', 'UPSERT',
            '--enable-hive-sync',
        ]
    )
