from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev

spark = (
    SparkSession.builder.appName("SparkConfigurationTest")
    .master("spark://localhost:7077")
    .config("spark.ui.port", "4050")
    .getOrCreate()
)

df = spark.range(10000).toDF("number")
df.show()

transformed_df = df.withColumn("number", col("number") * 5)

stats_df = transformed_df.agg(
    count("number").alias("count"),
    mean("number").alias("mean"),
    stddev("number").alias("stddev"),
)

stats_df.show()

spark.stop()


# park-submit --master spark://localhost:7077 spark-jobs/check/c1_spark_session.py
