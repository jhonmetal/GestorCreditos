"""
Feature: Data Analysis and Aggregation
  Scenario: Aggregate data for reporting
    Given a fact table with transaction data
    When the analysis process is run
    Then gold tables for top yearly accounts, top monthly accounts, and account master should be created
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

def test_gold_tables_created(spark):
    transform = Transform(spark)
    result = transform.analysis_data()
    gold_tables = [ConsETL.TOP_YEARLY_ACCOUNT_TABLE, ConsETL.TOP_MONTHLY_TABLE, ConsETL.ACCOUNT_MASTER_TABLE]
    found = set()
    for d in result:
        for k in d.keys():
            if k in gold_tables:
                found.add(k)
    assert set(gold_tables).issubset(found)
