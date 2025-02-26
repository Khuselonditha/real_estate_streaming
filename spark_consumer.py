import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType



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

if __name__ == "__main__":
    main()