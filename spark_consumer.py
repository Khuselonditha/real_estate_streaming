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
        StructField("title", StringType(), True),
        StructField("address", StringType(), True),
        StructField("price", StringType(), True),
        StructField("size", StringType(), True),
        StructField("bedrooms", StringType(), True),
        StructField("bathrooms", StringType(), True),
        StructField("parking", StringType(), True),
        StructField("link", StringType(), True),
        StructField("type of property", StringType(), True),
        StructField("lifestyle", StringType(), True),
        StructField("listing date", StringType(), True),
        StructField("levies", StringType(), True),
        StructField("no trasfer duty", StringType(), True),
        StructField("rates and taxes", StringType(), True),
        StructField("pets allowed", StringType(), True),
        StructField("pictures", ArrayType(StringType()), True)
    ])

if __name__ == "__main__":
    main()