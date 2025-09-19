FROM apache/airflow:slim-3.0.2-python3.10

# -----------------------------
# 4️⃣ Set proxy / noninteractive (if needed)
# -----------------------------
#ENV HTTP_PROXY=http://10.31.255.65:8080
#ENV HTTPS_PROXY=http://10.31.255.65:8080
#ENV http_proxy=http://10.31.255.65:8080
#ENV https_proxy=http://10.31.255.65:8080
ENV NO_PROXY=localhost,127.0.0.1

# Install java (openjdk)
USER root
RUN apt-get update && \
  apt-get install -y openjdk-17-jre-headless && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Configure Ivy settings for proxy
ENV IVY_SETTINGS="$AIRFLOW_HOME/ivy_settings.xml"

# Create Ivy settings file
RUN echo '<ivysettings><settings proxies="nexus-proxy"/><proxies><proxy id="nexus-proxy" org="10.31.255.65" host="10.31.255.65" port="8080" /></proxies></ivysettings>' > $AIRFLOW_HOME/ivy_settings.xml

# Set up more packages
#RUN pip install --proxy=http://10.31.255.65:8080 --trusted-host="10.179.0.20" apache-airflow-providers-apache-spark==5.3.1 pyspark==3.5.6 boto3

ENV HTTP_PROXY="http://10.31.255.65:8080"
ENV HTTPS_PROXY="http://10.31.255.65:8080"
ENV NO_PROXY="10.179.0.20,localhost,*.localhost,k3d_testme"

RUN pip install \
    --proxy=$HTTP_PROXY \
    --trusted-host="10.179.0.20" \
    apache-airflow-providers-apache-spark==5.3.1 pyspark==3.5.6 boto3

# Add a custom airflow database file path
RUN mkdir -p $AIRFLOW_HOME/data $AIRFLOW_HOME/cache

# Set environment variable untuk database Airflow dan Ivy cache
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////opt/airflow/data/airflow.db"
# ENV SPARK_SUBMIT_OPTS="-Divy.cache.dir=$AIRFLOW_HOME/cache -Divy.home=$AIRFLOW_HOME/cache"
# Set proxy for spark
ENV SPARK_OPTS="--driver-java-options \"-Dhttp.proxyHost=10.31.255.65 -Dhttp.proxyPort=8080 -Dhttps.proxyHost=10.31.255.65 -Dhttps.proxyPort=8080\""

ENV SPARK_SUBMIT_OPTS="-Divy.cache.dir=$AIRFLOW_HOME/cache -Divy.home=$AIRFLOW_HOME/cache"

RUN mkdir -p /opt/airflow/dags
COPY airflow/dags/ /opt/airflow/dags/
RUN mkdir -p /opt/airflow/jobs
COPY airflow/jobs/ /opt/airflow/jobs/
