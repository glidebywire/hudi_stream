# Lokasi file: dags/ingest_kafka_to_hudi_dag.py
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

PROPERTIES_FILE_PATH = "configs/kafka-hudi-kuponku-redeem.properties"
JARS_FILE_PATH = "hudi-mongo-transformer-1.0.0.jar"

with DAG(   
    dag_id="deltastreamer_kuponku_redeem",
    start_date=datetime(2025, 8, 8),
    schedule="@once", # wiwi can set timedelta(minutes=2) too
    catchup=False,
    tags=["hudi", "kafka", "delasteamer", "kuponku", "redemption"],
) as dag:
    ingest_task = SparkSubmitOperator(  
        task_id="deltastreamer_kuponku_redeem_task",
        application="jobs/jar/hudi-utilities-slim-bundle_2.12-1.0.2.jar", 
        conn_id="spark_default",
        java_class="org.apache.hudi.utilities.streamer.HoodieStreamer",
        jars = JARS_FILE_PATH,
        packages=(
                    'org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2,'
                    'org.apache.hadoop:hadoop-aws:3.3.4,'
                    # 'org.apache.spark:spark-avro_2.12:3.5.1,'     
                    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6'
                    # 'org.apache.avro:avro:1.11.3'
                ),
        # exclude_packages='org.apache.avro:avro',
        conf={
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
            'spark.executor.cores': '1',
            'spark.cores.max': '1',
            'spark.driver.extraJavaOptions': '-Dlog4j.rootCategory=DEBUG,console',
            'spark.executor.extraJavaOptions': '-Dlog4j.rootCategory=DEBUG,console'
            # 'spark.driver.userClassPathFirst': 'true',
            # 'spark.executor.userClassPathFirst': 'true'
        },
        application_args=[
            '--props', PROPERTIES_FILE_PATH,
            '--target-base-path', 's3a://advertising-data-lake/hudi-kuponku',
            '--target-table', 'kuponku_redemption',
            '--table-type', 'COPY_ON_WRITE',
            '--source-class', 'org.apache.hudi.utilities.sources.JsonKafkaSource',
            # '--schemaprovider-class', 'org.apache.hudi.utilities.schema.RowBasedSchemaProvider',
            '--schemaprovider-class', 'org.apache.hudi.utilities.schema.FilebasedSchemaProvider',   
            # '--schemaprovider-class', 'org.apache.hudi.utilities.schema.SchemaRegistryProvider',
            # '--schemaprovider-class', 'org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverter',
            '--op', 'UPSERT',
            '--transformer-class', 'com.aptana.hudi.transformers.MongoCdcTransformer',      
            '--enable-hive-sync',
        ]
    )
