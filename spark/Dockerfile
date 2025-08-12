FROM bitnami/spark:3.5.6

USER root

ENV SPARK_HOME=/opt/bitnami/spark
ENV ICEBERG_VERSION=1.9.2 \
    POSTGRES_JDBC_VERSION=42.7.7 \
    HADOOP_AWS_VERSION=3.3.6 \
    AWS_SDK_VERSION=1.12.696

WORKDIR ${SPARK_HOME}/jars

# Install Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Iceberg Spark runtime for Spark 3.5 (Scala 2.12)
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar .

# PostgreSQL JDBC driver (use latest patch in 42.7.x)
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar .

# Hadoop AWS and AWS SDK bundle (for S3A / MinIO)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar .
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar .


# (Optional) add parquet and avro if you want explicit versions
ADD https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.13.1/parquet-hadoop-1.13.1.jar .
ADD https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar .

# Fix perms and switch back to the default non-root user (Bitnami uses 1001)
RUN chown -R 1001:1001 ${SPARK_HOME}/jars || true

USER 1001

# App directory
WORKDIR /opt/spark-app

COPY main.py /opt/spark-app/main.py

# Default command runs your script with spark-submit
CMD ["spark-submit", "/opt/spark-app/main.py"]