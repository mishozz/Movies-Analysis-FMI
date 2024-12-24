from pyspark.sql import SparkSession

class SparkSessionManager:
    _instance = None

    @classmethod
    def get_session(cls):
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName("IMDb Analysis") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
        return cls._instance

    @classmethod
    def stop_session(cls):
        if cls._instance:
            cls._instance.stop()
            cls._instance = None
