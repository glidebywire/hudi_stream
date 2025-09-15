FROM apache/airflow:slim-3.0.2-python3.10

# Install java (openjdk)
USER root
RUN apt-get update && \
  apt-get install -y openjdk-17-jre-headless && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set up more packages
RUN pip install apache-airflow-providers-apache-spark==5.3.1 pyspark==3.5.6

# Add a custom airflow database file path
RUN mkdir -p $AIRFLOW_HOME/data $AIRFLOW_HOME/cache

# Set environment variable untuk database Airflow dan Ivy cache
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/data/airflow.db"
ENV SPARK_SUBMIT_OPTS="-Divy.cache.dir=$AIRFLOW_HOME/cache -Divy.home=$AIRFLOW_HOME/cache"

RUN mkdir -p /opt/airflow/dags
COPY airflow/dags/ /opt/airflow/dags/
RUN mkdir -p /opt/airflow/jobs
COPY airflow/jobs/ /opt/airflow/jobs/

