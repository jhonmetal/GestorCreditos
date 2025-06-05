from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List
from analytics.utils.cons_etl import ConsETL
from analytics.utils.util import Utils


@dataclass
class Ingest(Utils):
    """
    A class to handle data ingestion from CSV files into Spark DataFrames.

    Attributes:
        spark (SparkSession): The active Spark session.
        ingest_dfs (Dict[str, DataFrame]): A dictionary to store ingested DataFrames.
    """
    spark: SparkSession
    base_path: str
    ingest_dfs: Dict[str, DataFrame] = field(default_factory=dict)

    def _load_csv(self, path: str) -> DataFrame:
        """
        Helper method to load a CSV file as a DataFrame with predefined options.

        Args:
            path (str): The file path to the CSV file.

        Returns:
            DataFrame: The loaded DataFrame with schema inferred and headers included.
        """
        return self.spark.read.format('csv') \
            .option("header", True) \
            .option("inferSchema", True) \
            .load(path)

    def ingest_data(self) -> List[Dict[str, DataFrame]]:
        """
        Loads transaction, account, and customer data from CSV files into DataFrames,
        stores them in a dictionary, and returns the dictionary contents as a list of key-value pairs.

        Returns:
            List[Dict[str, DataFrame]]: A list of dictionaries containing the DataFrame names as keys
            and the corresponding DataFrames as values.
        """
        file_paths = {
            "transactions": ConsETL.TRANSACTIONS_PATH,
            "accounts": ConsETL.ACCOUNTS_PATH,
            "customers": ConsETL.CUSTOMERS_PATH
        }

        # Ingest data for each file path
        for key, path in file_paths.items():
            full_path = self.get_full_path(self.base_path, path)
            df = self._load_csv(full_path)
            self.ingest_dfs[key] = df
            df.limit(10).show()

        # Convert dictionary items to a list of key-value dictionaries
        list_df = [{key: value} for key, value in self.ingest_dfs.items()]

        return list_df
