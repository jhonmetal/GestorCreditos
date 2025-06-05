from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Optional

from analytics.utils.logger_wrapper import LoggerWrapper


@dataclass
class Persist:
    """
    A class to handle persisting Spark DataFrames to a specified database, with optional partitioning.

    Attributes:
        spark (SparkSession): The active Spark session.
    """
    spark: SparkSession
    logger: 'LoggerWrapper' = field(init=False, default_factory=lambda: LoggerWrapper())

    def persist_data(self, input_list: List[Dict[str, DataFrame]], db_name: str) -> None:
        """
        Persists a list of DataFrames into a specified database, using partitions if available.

        Args:
            input_list (List[Dict[str, DataFrame]]): A list containing dictionaries with DataFrame names as keys
                                                     and DataFrames as values.
            db_name (str): The database name where tables will be saved.

        Returns:
            None
        """
        self.logger.info('Persisting dataframes')
        try:
            for item in input_list:
                for table_name, df in item.items():
                    partitions = self._check_partition(db_name, table_name)
                    self._write_table(df, db_name, table_name, partitions)

        except Exception as err:
            self.logger.error(f"An error occurred while persisting data > {err}")

    @staticmethod
    def _write_table(df: DataFrame, db_name: str, table_name: str, partitions: Optional[List[str]]) -> None:
        """
        Writes a DataFrame as a table in the specified database with optional partitioning.

        Args:
            df (DataFrame): The DataFrame to be persisted.
            db_name (str): The target database name.
            table_name (str): The target table name.
            partitions (Optional[List[str]]): A list of partition column names if available.

        Returns:
            None
        """
        if partitions:
            df.write.mode('overwrite').partitionBy(*partitions).format("parquet").saveAsTable(f"{db_name}.{table_name}")
        else:
            df.write.mode('overwrite').format("parquet").saveAsTable(f"{db_name}.{table_name}")

    def _check_partition(self, db_name, table_name: str) -> Optional[List[str]]:
        """
        Checks if a DataFrame has partitions by describing the table schema,
        and returns a list of partition column names if available.

        Args:
            db_name (str): The target database name.
            table_name (str): The name of the table to check for partitions.

        Returns:
            Optional[List[str]]: A list of partition column names if they exist, otherwise None.
        """
        try:
            describe_df = self.spark.sql(f"DESCRIBE TABLE {db_name}.{table_name}").toPandas()
            partition_info = describe_df.loc[describe_df['col_name'] == '# Partition Information']
            if not partition_info.empty:
                partition_info = describe_df.loc[describe_df['col_name'] == '# Partition Information'].index[0] + 1
                partitions = describe_df['col_name'][partition_info:].tolist()
                return [column for column in partitions if column != '# col_name']
        except Exception as err:
            self.logger.error(f"An error occurred while checking for partition > {err}")
            return None

#HU-20101
