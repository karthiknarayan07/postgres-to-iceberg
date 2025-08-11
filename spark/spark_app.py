from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresToIceberg") \
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio.type", "hadoop") \
    .config("spark.sql.catalog.minio.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Read from Postgres with batching
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/ordersdb") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .option("fetchsize", 10000) \
    .load()

# Write to Iceberg partitioned by country, city
df.writeTo("minio.orders") \
    .partitionedBy("country", "city") \
    .createOrReplace()

spark.stop()
