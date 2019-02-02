from pyspark.sql import SparkSession

def create_session():
    spark = SparkSession.builder.master('local').appName("Yelp ETL Jobs").getOrCreate()
    return spark