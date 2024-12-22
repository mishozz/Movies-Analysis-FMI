from pyspark.sql import SparkSession

def create_spark_session(name):
    return SparkSession.builder \
        .appName(name) \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
