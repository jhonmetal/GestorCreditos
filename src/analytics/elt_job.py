import sys
from pyspark.sql import SparkSession
from analytics.pipeline import transform, load, ingest
from analytics.utils.cons_etl import ConsETL
from analytics.utils.log4j_logger import Log4jLogger
from analytics.utils.logger_wrapper import LoggerWrapper
from analytics.utils.util import Utils


class Pipeline(Utils):
    """
    Class to define and run the data processing pipeline.
    """

    def __init__(self, abs_path: str):
        """
        Initialize the Spark session and configure Hive support.
        Args:
            abs_path (str): The absolute base path for loading data and scripts.
        """
        self.base_path = abs_path
        self.spark = SparkSession.builder.appName(ConsETL.APP_NAME) \
            .config("spark.sql.warehouse.dir", "SparkWarehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        self.logger = None

    def run_pipeline(self):
        """
        Run the entire data processing pipeline including ingest, transform, and load.
        """
        self.logger = LoggerWrapper()
        try:
            self.logger.info('run_pipeline method started')
            ingest_process = ingest.Ingest(self.spark, self.base_path)
            persist_process = load.Persist(self.spark)
            # Ingestion phase and load dim_tables:
            ingest_df_list = ingest_process.ingest_data()
            persist_process.persist_data(ingest_df_list, ConsETL.BRONZE_DB)
            self.spark.sql(f"USE {ConsETL.BRONZE_DB};").show()
            self.spark.sql("SHOW TABLES;").show()

            # Transform phase and load fact_table:
            transform_process = transform.Transform(self.spark)
            trans_df_list = transform_process.transform_data()
            persist_process.persist_data(trans_df_list, ConsETL.SILVER_DB)
            self.spark.sql(f"USE {ConsETL.SILVER_DB};").show()
            self.spark.sql("SHOW TABLES;").show()

            # Transform phase and load analysis table:
            analysis_df_list = transform_process.analysis_data()
            persist_process.persist_data(analysis_df_list, ConsETL.GOLD_DB)
            self.spark.sql(f"USE {ConsETL.GOLD_DB};").show()
            self.spark.sql("SHOW TABLES;").show()
            self.logger.info('run_pipeline method ended')
        except Exception as exp:
            self.logger.info(f"An error occurred while running the pipeline. Got error: {exp}")
            sys.exit(1)

        return

    def create_hive_database(self):
        """
        Create Hive databases for bronze, silver and gold data.
        """

        schema_path = self.get_full_path(self.base_path, ConsETL.DDL_PATH)
        with open(schema_path, "r") as file:
            ddl_statements = file.read()

        ddl_statements = eval(f"f'''{ddl_statements}'''")

        for statement in ddl_statements.split(';'):
            if statement.strip():
                self.spark.sql(f"{statement}")

        self.spark.sql("SHOW DATABASES;").show()


def main():
    print('Application started')
    abs_path = sys.argv[1] if len(sys.argv) > 1 else None
    pipeline = Pipeline(abs_path)
    log4j_logger = Log4jLogger(spark=pipeline.spark)
    LoggerWrapper(log4j_logger)
    pipeline.create_hive_database()
    print('Spark Session created')
    pipeline.run_pipeline()
    print('Pipeline executed')
    print('For questions and support send a mail to: jhonathan.pauca@unmsm.edu.pe')

if __name__ == '__main__':
    main()

#HU-20101