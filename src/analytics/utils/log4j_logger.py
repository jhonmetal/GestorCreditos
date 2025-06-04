from pyspark.sql import SparkSession

class Log4jLogger:
    """Spark logging class"""
    def __init__(self, spark: SparkSession):
        conf = spark.sparkContext.getConf()
        app_name = conf.get('spark.app.name')
        message_prefix = f'< {app_name} >'
        self.logger = spark._jvm.org.apache.log4j.LogManager.getLogger(message_prefix)

    def info(self, msg: str):
        self.logger.info(msg)

    def error(self, msg: str):
        self.logger.error(msg)

    def warn(self, msg: str):
        self.logger.warn(msg)
