from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LocalSparkExample") \
    .master("local[*]") \
    .getOrCreate()

data = ["Hello Spark", "Spark is great", "Hello World"]
rdd = spark.sparkContext.parallelize(data)

word_counts = (rdd.flatMap(lambda line: line.split(" "))
                   .map(lambda word: (word, 1))
                   .reduceByKey(lambda a, b: a + b))

print(word_counts.collect())

spark.stop()
