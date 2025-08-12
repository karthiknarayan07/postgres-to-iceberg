from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import traceback


def main():
    spark = None
    try:
        print("Starting Spark application...")
        spark = (
            SparkSession.builder.appName("PostgresToIceberg")
            .config(
                "spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.my_catalog.type", "hadoop")
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.metadata.store.impl",
                "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore",
            )
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )

        print("Spark session created successfully")

        print("Reading from Postgres...")
        # Read from Postgres with batching
        df = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/ordersdb")
            .option("dbtable", "orders")
            .option("user", "admin")
            .option("password", "admin")
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", 10000)
            .load()
        )

        print(f"Read {df.count()} records from Postgres")
        df.show(5)

        print("Writing to Iceberg...")
        # Write to Iceberg partitioned by country, city
        df.writeTo("my_catalog.default.orders").partitionedBy(
            col("country"), col("city")
        ).createOrReplace()

        print("Successfully wrote data to Iceberg table")

        # Verify the write
        result_df = spark.table("my_catalog.default.orders")
        print(f"Verification: Iceberg table contains {result_df.count()} records")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        traceback.print_exc()
        if spark:
            spark.stop()
        sys.exit(1)

    finally:
        if spark:
            print("Stopping Spark session...")
            spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    main()
