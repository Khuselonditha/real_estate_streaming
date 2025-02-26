import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    session.execute("""
        CREATE IF NOT EXISTS property_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
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
            link TEXT PRIMARY KEY,
            type_of_property TEXT,
            lifestyle TEXT,
            listing_date TEXT,
            levies TEXT,
            no_transfer_duty TEXT,
            rates_and_taxes TEXT,
            pets_allowed TEXT,
            pictures list<TEXT>
        );          
    """)

    logging.info("Table created successfully.")


def insert_data(session, **kwargs):
    logging.info("Inserting data...")

    query = """
        INSERT INTO property_streams.properties(title, address, price, size, 
            bedrooms, bathrooms, parking, link, type_of_property, lifestyle, 
            listing_date, levies, no_transfer_duty, rates_and_taxes, pets_allowed, 
            pictures)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        session.execute(query, **kwargs.values())
        logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Failed to insert data: {e}")
        raise


def create_cassandra_session():
    try:
        session = Cluster(['localhost']).connect()

        if session is not None:
            create_keyspace(session)
            create_table(session)
        
        return session

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
            .option("startingOffsets", "earliest")
            .load())
    
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("address", StringType(), True),
        StructField("price", StringType(), True),
        StructField("size", StringType(), True),
        StructField("bedrooms", StringType(), True),
        StructField("bathrooms", StringType(), True),
        StructField("parking", StringType(), True),
        StructField("link", StringType(), True),
        StructField("type_of_property", StringType(), True),
        StructField("lifestyle", StringType(), True),
        StructField("listing_date", StringType(), True),
        StructField("levies", StringType(), True),
        StructField("no_trasfer_duty", StringType(), True),
        StructField("rates_and_taxes", StringType(), True),
        StructField("pets_allowed", StringType(), True),
        StructField("pictures", ArrayType(StringType()), True)
    ])

    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING) as value")
           .select(from_json(col("value"), schema).alias("data"))
           .select("data.*"))
    
    cassandra_query = (kafka_df.writeStream
                        .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(
                           lambda row: insert_data(create_cassandra_session(), **row.asDict())))
                        .start()
                        .awaitTermination()
                       )



if __name__ == "__main__":
    main()