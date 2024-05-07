# Spark streaming job to read data from kafka and write it to cassandra
import logging

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.getLogger("kafka").setLevel(logging.INFO)


# Keyspace and table creation = like creating a database and a table in SQL
def create_keysapce(session):
    # create keyspace here
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS spark_streams\
                    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    print("Keyspace created successfully")


# Table creation = like creating a table in SQL
def create_table(session):
    # create table here
    session.execute("""CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
   """)

    print("Table created successfully")


# Insert data into the table that we are fetching from kafka
def insert_data(session, **kwargs):
    # insertion here
    print("inserting data...")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("post_code")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                user_id,
                first_name,
                last_name,
                gender,
                address,
                postcode,
                email,
                username,
                dob,
                registered_date,
                phone,
                picture,
            ),
        )
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")


# Establish connection with Kafka Spark and Cassandra so we need to create a function for each
def create_spark_connection():
    # creating spark connection
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.cassandra.auth.username", "cassandra")
            .config("spark.cassandra.auth.password", "cassandra")
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


def create_cassandra_connection():
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
    # creating cassandra connection
    try:
        # Connecting to the cassandra cluster
        cluster = Cluster(
            ["localhost"],
            auth_provider=auth_provider,
        )

        cas_session = cluster.connect("spark_streams")
        logging.info("Cassandra connection established successfully")
        return cas_session
    except Exception as e:
        logging.error(f"Couldn't connect to the cassandra cluster due to: {e}")
        return None


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
            session = create_cassandra_connection()

            if session is not None:
                create_keysapce(session)
                create_table(session)

                logging.info("Streaming is being started...")

                streaming_query = (
                    selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option("keyspace", "spark_streams")
                    .option("table", "created_users")
                    .start()
                )

         
                streaming_query.awaitTermination(timeout=180)

                logging.info("Streaming is done")

# To run this script: python realtime-data-streaming/spark-jobs/spark_cassandra.py
# to see the Spark UI http://localhost:9090
# and the Cassandra data, run the following command: docker exec -it cassandra cqlsh -u cassandra -p casssandra localhost 9042

# spark-submit --master spark://localhost:7077 realtime-data-streaming/spark-jobs/spark_cassandra.py
# Dependencies error:
# The error is due to the missing dependencies in the spark-submit command. The command needs to include the necessary dependencies for the Spark job to run successfully.
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --master spark://localhost:7077 realtime-data-streaming/spark-jobs/spark_casssandra.py
