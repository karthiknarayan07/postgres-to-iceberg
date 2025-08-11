FROM bitnami/spark:latest
USER root

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/spark-app
CMD ["spark-submit", "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.postgresql:postgresql:42.7.3", "spark_app.py"]
