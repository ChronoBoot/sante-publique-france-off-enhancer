from pyspark import SparkConf
from pyspark.sql import SparkSession


class SingletonSparkSession:
    _instance: SparkSession = None

    @staticmethod
    def get_instance() -> SparkSession:
        if SingletonSparkSession._instance is None:
            SingletonSparkSession()
        return SingletonSparkSession._instance

    def __init__(self):
        if SingletonSparkSession._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            # noinspection SpellCheckingInspection
            conf = SparkConf() \
                .setAppName("SantePubliqueFranceOffEnhancer") \
                .set('spark.executor.memory', '4g') \
                .set('spark.driver.memory', '2g')
            SingletonSparkSession._instance = SparkSession.builder.config(conf=conf).getOrCreate()
