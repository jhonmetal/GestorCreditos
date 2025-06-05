"""
Feature: Data Ingestion
  Scenario: Ingest CSV files into DataFrames
    Given valid CSV files for transactions, accounts, and customers
    When the ingestion process is run
    Then DataFrames for each entity should be created and not be empty
"""
import pytest
from pyspark.sql import SparkSession
from analytics.pipeline.ingest import Ingest
from analytics.utils.cons_etl import ConsETL

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()
    yield spark
    spark.stop()

def test_ingest_dataframes_created(spark, tmp_path):
    # Setup: Copy sample CSVs to tmp_path if needed, or use provided paths
    ingest = Ingest(spark, base_path=".")
    result = ingest.ingest_data()
    keys = {list(d.keys())[0] for d in result}
    assert {"transactions", "accounts", "customers"}.issubset(keys)
    for d in result:
        for df in d.values():
            assert df.count() > 0
