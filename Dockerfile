FROM apache/airflow:2.8.1-python3.10

USER root

# 1. Cài đặt Java 17 (Bắt buộc cho Spark) và các công cụ hệ thống
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        curl \
        gcc \
        python3-dev \
        libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Cài đặt Spark 3.5.0
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark

# 3. Tải sẵn JARs Kafka cho Spark (Tránh download lúc runtime gây lag)
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.12.0.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# 4. Thiết lập biến môi trường
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/home/airflow/.local/bin:${SPARK_HOME}/bin:${PATH}"

USER airflow

# 5. Cài đặt tách biệt để tránh xung đột dependency
# Bước 5a: Cài đặt các gói Provider của Airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-postgres" \
    "apache-airflow-providers-google" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

# Bước 5b: Cài đặt các thư viện ngoài cho DAGs và dbt
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    confluent-kafka \
    pandas \
    vnstock==0.2.9.2 \
    pyarrow \
    fsspec \
    google-cloud-storage \
    dbt-bigquery \
    pydantic-settings \
    psycopg2-binary