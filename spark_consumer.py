import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    session.execute("""
        CREATE IF NOT EXISTS property_streams
        WITH replication = {'class': 'SimpleStrategy', replication_factor: '1'};
    """)

    logging.info("Keyspace created successfully.")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS property_streams.properties(
            title TEXT,
            address TEXT,
            price TEXT,
            size TEXT,
            bedrooms TEXT,
            bathrooms TEXT,
            parking TEXT,
            link TEXT,
            type_of_property TEXT,
            lifestyle TEXT,
            listing_date TEXT,
            levies TEXT,
            no_transfer_duty TEXT,
            rates_and_taxes TEXT,
            pets_allowed TEXT,
            pictures list<TEXT>,
            PRIMARY KEY (link)
        );          
    """)

    logging.info("Table created successfully.")


def create_cassandra_session():
    try:
        session = Cluster(['localhost']).connect()

        if session is not None:
            create_keyspace(session)
            create_table(session)

    except Exception as e:
        logging.error(f"Couldn't create the cassandra connection due to {e}")

        return None


def main():
    # Logging config
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    spark_conn = (SparkSession.builder
            .appName('RealEstateConsumer') 
            .config('spark.cassandra.connection.host', 'localhost')
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .getOrCreate())
    
    kafka_df = (spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "properties")
            .option("startingOffests", "earliest")
            .load())
    
    schema = StructType([
        StructField("Title", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Price", StringType(), True),
        StructField("Size", StringType(), True),
        StructField("Bedrooms", StringType(), True),
        StructField("Bathrooms", StringType(), True),
        StructField("Parking", StringType(), True),
        StructField("Link", StringType(), True),
        StructField("Type of Property", StringType(), True),
        StructField("Lifestyle", StringType(), True),
        StructField("Listing Date", StringType(), True),
        StructField("levies", StringType(), True),
        StructField("No Trasfer Duty", StringType(), True),
        StructField("Rates and Taxes", StringType(), True),
        StructField("Pets Allowed", StringType(), True),
        StructField("Pictures", ArrayType(StringType()), True)
    ])

    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING) as value")
           .select(from_json(col("value"), schema).alias("data"))
           .select("data.*"))
    
    cassandra_query = (kafka_df.writeStream
                       .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(
                           lambda row: insert_data(cassandra_session(), **row.asDict())))
                       )



if __name__ == "__main__":
    main()