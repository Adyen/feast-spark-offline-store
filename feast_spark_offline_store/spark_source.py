import pickle
import random
import string
from enum import Enum
from typing import Optional, Dict, Callable, Any, Tuple, Iterable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

from feast_spark_offline_store.spark_type_map import spark_to_feast_value_type


class SparkSourceFormat(Enum):
    # avro = "avro"  # tests.test_spark_source.test_spark_source_path fails
    csv = "csv"
    json = "json"
    # ost = "ost"  # tests.test_spark_source.test_spark_source_path fails
    parquet = "parquet"


class SparkSource(DataSource):
    """
    A data source that retrieves pyspark dataframes for SparkOfflineStore.

    One should specify either table, or query, or path. If more than one of these three
    is specified an IllegalArgumentException is raised.

    Args:
        table (optional): Name of specified table to load view as dataframe from.
        query (optional): Spark SQL query which should return view as dataframe.
        path (optional): Path that contains file(s) from which view will be loaded.
        file_format (optional): File format for reading 'path', must be one of
            SparkSourceFormat, required if 'path' is specified, ignored otherwise.
        event_timestamp_column (optional): Event timestamp column used for point in
            time joins of feature values.
        query (optional): The query to be executed to obtain the features.
        created_timestamp_column (optional): Timestamp column indicating when the
            row was created, used for de-duplicating rows.
        field_mapping (optional): A dictionary mapping of column names in this data
            source to column names in a feature table or view.
        date_partition_column (optional): Timestamp column used for partitioning.
    """

    def __init__(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        file_format: Optional[str] = None,
        # jdbc=None,  # TODO support jdbc reader
        # options: Optional[Dict[str, Any]] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
    ):
        if 1 != self.count_nonnull(table, query, path):
            raise ValueError("Exactly one of 'table','query','path' must be specified")
        if path is not None:
            if file_format is None:
                raise ValueError(
                    "If 'path' is specified, then 'file_format' is required"
                )
            allowed_formats = [f.value for f in SparkSourceFormat]
            if file_format not in allowed_formats:
                raise ValueError(f"'file_format' should be one of {allowed_formats}")

        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._spark_options = SparkOptions(
            table=table,
            query=query,
            path=path,
            file_format=file_format,
        )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        # TODO see feast.type_map for examples
        return spark_to_feast_value_type

    @staticmethod
    def count_nonnull(*args) -> bool:
        return sum([(arg is not None) for arg in args])

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from feast_spark_offline_store.spark import (
            get_spark_session_or_start_new_with_repoconfig,
        )  # import is here to avoid circular dependency

        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        df = spark_session.sql(f"SELECT * FROM {self.get_table_query_string()}")
        try:
            return (
                (fields["name"], fields["type"])
                for fields in df.schema.jsonValue()["fields"]
            )
        except AnalysisException:
            raise DataSourceNotFoundException()  # TODO: review error handling

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f"`{self.table}`"
        if self.query:
            return f"({self.query})"

        # If both table and query are null, this means we load from a file
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise AssertionError("Could not find an active spark session")
        df = spark_session.read.format(self.file_format).load(self.path)

        tmp_view_name = format_tmp_table_name(
            seed=self.path  # seed from path to have deterministic tmp_view_name
        )
        df.createOrReplaceTempView(tmp_view_name)

        return f"`{tmp_view_name}`"

    @property
    def spark_options(self):
        """
        Returns the spark options of this data source
        """
        return self._spark_options

    @spark_options.setter
    def spark_options(self, spark_options):
        """
        Sets the spark options of this data source
        """
        self._spark_options = spark_options

    @property
    def table(self):
        """
        Returns the table of this feature data source
        """
        return self._spark_options.table

    @property
    def query(self):
        """
        Returns the query of this feature data source
        """
        return self._spark_options.query

    @property
    def path(self):
        """
        Returns the query of this feature data source
        """
        return self._spark_options.path

    @property
    def file_format(self):
        """
        Returns the query of this feature data source
        """
        return self._spark_options.file_format

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        assert data_source.HasField("custom_options")
        spark_options = SparkOptions.from_proto(data_source.custom_options)

        return SparkSource(
            table=spark_options.table,
            query=spark_options.query,
            path=spark_options.path,
            file_format=spark_options.file_format,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self.spark_options.to_proto(),
        )
        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto


class SparkOptions:
    """
    Config class that holds custom options used by the SparkSource class.

    Please note the options are not for the spark session itself, but rather for how a
    spark session can load the data related to the SparkSource.
    """

    def __init__(self, table: str, query: str, path: str, file_format: str):
        self._table = table
        self._query = query
        self._path = path
        self._file_format = file_format

    @property
    def table(self):
        """
        Returns the table
        """
        return self._table

    @table.setter
    def table(self, table):
        """
        Sets the table
        """
        self._table = table

    @property
    def query(self):
        """
        Returns the query
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the query
        """
        self._query = query

    @property
    def path(self):
        """
        Returns the path
        """
        return self._path

    @path.setter
    def path(self, path):
        """
        Sets the path
        """
        self._path = path

    @property
    def file_format(self):
        """
        Returns the file_format
        """
        return self._file_format

    @file_format.setter
    def file_format(self, file_format):
        """
        Sets the file_format
        """
        self._file_format = file_format

    @classmethod
    def from_proto(cls, spark_options_proto: DataSourceProto.CustomSourceOptions):
        """
        Creates a SparkOptions from a protobuf representation of a spark option

        args:
            spark_options_proto: a protobuf representation of a datasource

        Returns:
            Returns a SparkOptions object based on the spark_options protobuf
        """
        spark_configuration = pickle.loads(spark_options_proto.configuration)
        spark_options = cls(
            table=spark_configuration.table,
            query=spark_configuration.query,
            path=spark_configuration.path,
            file_format=spark_configuration.file_format,
        )
        return spark_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts an SparkOptionsProto object to its protobuf representation.

        Returns:
            SparkOptionsProto protobuf
        """
        spark_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=pickle.dumps(self),
        )
        return spark_options_proto


def format_tmp_table_name(size=15, chars=string.ascii_letters, seed=None) -> str:
    if seed:
        random.seed(seed)
    return "".join(random.choice(chars) for _ in range(size))
