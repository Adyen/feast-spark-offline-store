from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pyspark.sql import SparkSession, DataFrame

from feast_spark_offline_store import SparkSource
from feast_spark_offline_store.spark_source import (
    format_tmp_table_name,
    SparkSourceFormat,
)


def test_spark_source_table(spark_session: SparkSession, example_df):
    # Register an example dataframe as table in the active spark session
    tmp_table_name = format_tmp_table_name()
    example_df.createOrReplaceTempView(tmp_table_name)

    # Create source with table arg
    source = SparkSource(table=tmp_table_name)

    # Try to fetch data
    df = spark_session.sql(f"SELECT * FROM {source.get_table_query_string()}")
    assert df is not None
    assert df.count() > 0

    # Test protobuf (de)serializing
    assert source == SparkSource.from_proto(source.to_proto())


def test_spark_source_query(spark_session: SparkSession, example_df):
    # Register an example dataframe as table in the active spark session
    tmp_table_name = format_tmp_table_name()
    example_df.createOrReplaceTempView(tmp_table_name)

    # Create source with query arg
    source = SparkSource(query=f"SELECT * FROM {tmp_table_name}")

    # Try to fetch data
    df = spark_session.sql(f"SELECT * FROM {source.get_table_query_string()}")
    assert df is not None
    assert df.count() > 0

    # Test protobuf (de)serializing
    assert source == SparkSource.from_proto(source.to_proto())


@pytest.mark.parametrize("file_format", [s.value for s in SparkSourceFormat])
def test_spark_source_path(
    spark_session: SparkSession, example_df: DataFrame, file_format: str, root_path: str
):
    with TemporaryDirectory(dir=root_path) as tmp_dir:
        example_df_path = str(Path(tmp_dir) / "example_df")

        # Write df in specified format
        example_df.write.format(file_format).save(example_df_path)

        # Create source with query arg
        source = SparkSource(path=example_df_path, file_format=file_format)

        # Try to fetch data
        df = spark_session.sql(f"SELECT * FROM {source.get_table_query_string()}")
        assert df is not None
        assert df.count() > 0

        # Test protobuf (de)serializing
        assert source == SparkSource.from_proto(source.to_proto())


def test_no_args_raises():
    try:
        SparkSource()  # no args
        assert False, "Should not be possible to create a SparkSource without any args"
    except ValueError:
        assert True


def test_both_table_and_query_args_raises():
    try:
        SparkSource(table="foo", query="bar")  # both table and query
        assert False
    except ValueError:
        assert True


def test_both_table_and_path_args_raises():
    try:
        SparkSource(table="foo", path="bar")  # both table and path
        assert False
    except ValueError:
        assert True


def test_both_query_and_path_args_raises():
    try:
        SparkSource(query="foo", path="bar")  # both query and path
        assert False
    except ValueError:
        assert True


def test_both_path_without_file_format():
    try:
        SparkSource(path="bar", file_format=None)
        assert False
    except ValueError:
        assert True
