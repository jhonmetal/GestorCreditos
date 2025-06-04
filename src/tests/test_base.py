import pytest
from pyspark.sql import SparkSession
from analytics.utils.util import Utils


@pytest.fixture(scope="module")
def spark():
    # Create a Spark session for testing
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("unit-tests") \
        .getOrCreate()
    yield spark
    spark.stop()  # Stop the Spark session after tests are done


def test_count_nulls(spark):
    # Create a simple DataFrame with null values
    data = [(1, None), (2, "foo"), (3, "bar")]
    columns = ["id", "name"]
    df = spark.createDataFrame(data, columns)

    # Call your method on the DataFrame
    result_df = Utils.count_nulls(df)

    # Collect results and validate
    result = result_df.collect()

    # Assert the count of null values
    assert result[0][0] == 1  # id_null_count should be 1
    assert result[1][0] == 0  # name_null_count should be 0