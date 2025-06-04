"""
Feature: Null and Blank Value Handling
  Scenario: Count and percentage of null/blank values
    Given a DataFrame with null and blank values
    When the utility functions are called
    Then the correct count and percentage of null/blank values should be returned for each column
"""
import pytest
from pyspark.sql import SparkSession
from analytics.utils.util import Utils

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()
    yield spark
    spark.stop()

def test_null_and_blank_counts(spark):
    data = [(1, None, ""), (2, "foo", "bar"), (3, None, "baz")]
    columns = ["id", "name", "desc"]
    df = spark.createDataFrame(data, columns)
    null_counts = Utils.count_nulls(df).collect()[0]
    null_blank_counts = Utils.count_nulls_and_blanks(df).collect()[0]
    percent_null_blank = Utils.percentage_nulls_and_blanks(df).collect()[0]
    assert null_counts[1] == 2  # name has 2 nulls
    assert null_blank_counts[2] == 1  # desc has 1 blank
    assert percent_null_blank[2] >= 0  # percentage is calculated
