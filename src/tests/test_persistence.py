"""
Feature: Data Persistence
  Scenario: Persist DataFrames to Hive tables
    Given DataFrames ready for persistence
    When the persist process is run
    Then the corresponding Hive tables should be created and populated
"""
import pytest
from pyspark.sql import SparkSession
from analytics.pipeline.load import Persist
from analytics.utils.cons_etl import ConsETL

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("unit-tests").enableHiveSupport().getOrCreate()
    yield spark
    spark.stop()

def test_persist_creates_tables(spark):
    persist = Persist(spark)
    # For test, create a dummy DataFrame
    df = spark.createDataFrame([(1, "foo")], ["id", "name"])
    persist.persist_data([{"test_table": df}], ConsETL.BRONZE_DB)
    tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {ConsETL.BRONZE_DB}").collect()]
    assert "test_table" in tables
