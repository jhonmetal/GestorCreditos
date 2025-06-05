"""
Feature: Data Transformation
  Scenario: Transform and join ingested data
    Given ingested DataFrames for transactions, accounts, and customers
    When the transformation process is run
    Then a fact table should be created with correct columns and joined data
"""
import pytest
from pyspark.sql import SparkSession
from analytics.pipeline.transform import Transform
from analytics.utils.cons_etl import ConsETL

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()
    yield spark
    spark.stop()

def test_fact_table_columns(spark):
    transform = Transform(spark)
    result = transform.transform_data()
    fact_table = None
    for d in result:
        if ConsETL.FACT_TABLE in d:
            fact_table = d[ConsETL.FACT_TABLE]
    assert fact_table is not None
    expected_cols = ["transaction_id", "account_id", "customer_id", "name", "transaction_date", "transaction_amount", "transaction_type", "account_type", "balance", "registration_date", "cutoff_date"]
    for col in expected_cols:
        assert col in fact_table.columns
