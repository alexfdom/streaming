# Spark streaming job to read data from kafka and write it to cassandra
import logging
import os
from dotenv import dotenv_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.getLogger("kafka").setLevel(logging.INFO)

# Load environment variables
script_file = os.path.realpath(__file__)
script_path = os.path.dirname(script_file)
env_file_path = os.path.join(script_path, ".env")
config = dotenv_values(env_file_path)

# Establish connection with Kafka Spark and Cassandra so we need to create a function for each
def create_spark_connection():
    # creating spark connection
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.master("local[2]").appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
            )
            .config("spark.mongodb.input.uri", config["MONGO_URI"])
            .config("spark.mongodb.output.uri", f"{config["MONGO_URI"]}/{config["MONGO_DB"]}/{config["MONGO_COLLECTION"]}")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully")
    except Exception as e:
        logging.error(f"Couldn't create a spark session due to: {e}")

    return s_conn


def connect_to_kafka(spark_conn, kafka_bootstrap_servers, kafka_topic):
    # connecting to kafka
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created due to: {e}")

    return spark_df



def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    print(sel)

    return sel


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Create a spark connection
    spark_conn = create_spark_connection()

    # Create a cassandra connection
    if spark_conn is not None:
        # connect to kafka and get the data with spark connection
        spark_df = connect_to_kafka(spark_conn, "localhost:9092", "user_created")

        if spark_df is not None:
            # structure the data to fix the cassandra table schema
            selection_df = create_selection_df_from_kafka(spark_df)


            logging.info("Streaming is being started...")

            streaming_query = (
                selection_df.writeStream.format("mongo")
                .option("checkpointLocation", "/tmp/checkpoint")
                .trigger(processingTime='5 seconds')
                .foreachBatch(lambda df, epoch_id: df.write.format("mongo")
                  .mode("append")
                  .option("database", config["MONGO_DB"])
                  .option("collection", config["MONGO_COLLECTION"])
                  .save())
                .start()
            )

        
            streaming_query.awaitTermination(timeout=180)

            logging.info("Streaming is done")

# To run this script: python realtime-data-streaming/spark-jobs/spark_mongo.py
# to see the Spark UI http://localhost:9090
# and the Cassandra data, run the following command: docker exec -it cassandra cqlsh -u cassandra -p casssandra localhost 9042

# spark-submit --master spark://localhost:7077 realtime-data-streaming/spark-jobs/spark_mongo.py
# Dependencies error:
# The error is due to the missing dependencies in the spark-submit command. The command needs to include the necessary dependencies for the Spark job to run successfully.
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --master spark://localhost:7077 realtime-data-streaming/spark-jobs/spark_mongo.py