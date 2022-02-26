import inspect
from typing import List, Union, Optional, Dict, Tuple
from ntpath import join
from pydantic import StrictStr
from datetime import datetime

import uuid
import pandas
from dateutil import parser
import pyspark
import pyarrow
import numpy as np
import pandas as pd
from pydantic import StrictStr
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pytz import utc

from feast import FeatureView, OnDemandFeatureView
from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage

import pandas as pd

from feast.repo_config import FeastConfigBaseModel

from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob, RetrievalMetadata
from feast.infra.offline_stores import offline_utils

from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

from feast.errors import InvalidEntityType
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.offline_utils import FeatureViewQueryContext
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

from pyspark.sql import SparkSession
from pyspark import SparkConf
from feast_spark_offline_store.spark_source import SparkSource, SavedDatasetSparkStorage
from feast_spark_offline_store.spark_type_map import spark_schema_to_np_dtypes


class SparkOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "spark"
    """ Offline store type selector"""

    spark_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the spark session """
    # to ensure sparksession is the correct config, if not created yet
    # sparksession is not serializable and we dont want to pass it around as an argument


class SparkOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        spark_session = get_spark_session_or_start_new_with_repoconfig(
            config.offline_store
        )
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        assert isinstance(data_source, SparkSource)

        print("Pulling latest features from spark offline store")

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamps)

        start_date = _format_datetime(start_date)
        end_date = _format_datetime(end_date)
        query = f"""
                SELECT
                    {field_string}
                    {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
                FROM (
                    SELECT {field_string},
                    ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS feast_row_
                    FROM {from_expression} t1
                    WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
                ) t2
                WHERE feast_row_ = 1
                """

        return SparkRetrievalJob(
            spark_session=spark_session,
            query=query,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        tmp_entity_df_table_name = offline_utils.get_temp_entity_table_name()

        entity_schema = _upload_entity_df_and_get_entity_schema(
            spark_session=spark_session,
            table_name=tmp_entity_df_table_name,
            entity_df=entity_df,
        )
        event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema=entity_schema,
        )
        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, event_timestamp_col, spark_session,
        )

        expected_join_keys = offline_utils.get_expected_join_keys(
            project=project, feature_views=feature_views, registry=registry
        )
        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema=entity_schema,
            join_keys=expected_join_keys,
            entity_df_event_timestamp_col=event_timestamp_col,
        )

        query_context = offline_utils.get_feature_view_query_context(
            feature_refs,
            feature_views,
            registry,
            project,
            entity_df_event_timestamp_range,
        )

        query = offline_utils.build_point_in_time_query(
            feature_view_query_contexts=query_context,
            left_table_query_string=tmp_entity_df_table_name,
            entity_df_event_timestamp_col=event_timestamp_col,
            entity_df_columns=entity_schema.keys(),
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )
        on_demand_feature_views = OnDemandFeatureView.get_requested_odfvs(
            feature_refs=feature_refs, project=project, registry=registry
        )

        return SparkRetrievalJob(
            spark_session=spark_session,
            query=query,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_schema.keys()) - {event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        """
        Note that join_key_columns, feature_name_columns, event_timestamp_column, and created_timestamp_column
        have all already been mapped to column names of the source table and those column names are the values passed
        into this function.
        """
        return SparkOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns
            + [event_timestamp_column],  # avoid deduplication
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=None,
            start_date=start_date,
            end_date=end_date,
        )

# TODO fix internal abstract methods _to_df_internal _to_arrow_internal
class SparkRetrievalJob(RetrievalJob):
    def __init__(
        self,
        spark_session: SparkSession,
        query: str,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]],
        metadata: Optional[RetrievalMetadata] = None,
    ):
        super().__init__()
        self.spark_session = spark_session
        self.query = query
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def to_spark_df(self) -> pyspark.sql.DataFrame:
        statements = self.query.split(
            "---EOS---"
        )  # TODO can do better than this dirty split
        *_, last = map(self.spark_session.sql, statements)
        return last

    def to_df(self) -> pandas.DataFrame:
        return self.to_spark_df().toPandas()  # noqa, DataFrameLike instead of DataFrame

    def _to_df_internal(self) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously"""
        return self.to_df()

    def _to_arrow_internal(self) -> pyarrow.Table:
        """Return dataset as pyarrow Table synchronously"""
        return self.to_arrow()

    def to_arrow(self) -> pyarrow.Table:
        df = self.to_df()
        return pyarrow.Table.from_pandas(df)  # noqa

    def persist(self, storage: SavedDatasetStorage):
        """
        Run the retrieval and persist the results in the same offline store used for read.
        """
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """
        Return metadata information about retrieval.
        Should be available even before materializing the dataset itself.
        """
        return self._metadata


def get_spark_session_or_start_new_with_repoconfig(
    store_config: SparkOfflineStoreConfig,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        spark_builder = SparkSession.builder
        spark_conf = store_config.spark_conf
        if spark_conf:
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll(spark_conf.items())
            )  # noqa

        spark_session = spark_builder.getOrCreate()
    spark_session.conf.set(
        "spark.sql.parser.quotedRegexColumnNames", "true"
    )  # important!
    return spark_session


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    spark_session: SparkSession,
) -> Tuple[datetime, datetime]:
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        df = spark_session.sql(entity_df).select(entity_df_event_timestamp_col)
        entity_df_event_timestamp_range = (
            df.agg({entity_df_event_timestamp_col: "max"}).collect()[0][0],
            df.agg({entity_df_event_timestamp_col: "min"}).collect()[0][0]
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range

def _upload_entity_df_and_get_entity_schema(
    spark_session: SparkSession,
    table_name: str,
    entity_df: Union[pandas.DataFrame, str],
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        spark_session.createDataFrame(entity_df).createOrReplaceTempView(table_name)
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        spark_session.sql(entity_df).createOrReplaceTempView(table_name)
        limited_entity_df = spark_session.table(table_name)
        return dict(
            zip(
                limited_entity_df.columns,
                spark_schema_to_np_dtypes(limited_entity_df.dtypes),
            )
        )
    else:
        raise InvalidEntityType(type(entity_df))

def _format_datetime(t: datetime):
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=utc)
    t = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return t


    return spark_session


def _format_datetime(t: datetime):
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=utc)
    t = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return t


def _get_feature_view_query_context(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    feature_refs: List[str],
    feature_views: List[FeatureView],
    spark_session: SparkSession,
    table_name: str,
    registry: Registry,
    project: str,
) -> List[FeatureViewQueryContext]:
    # interface of offline_utils.get_feature_view_query_context changed in feast==0.17
    arg_spec = inspect.getfullargspec(func=offline_utils.get_feature_view_query_context)
    if "entity_df_timestamp_range" in arg_spec.args:
        # for feast>=0.17
        entity_df_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df=entity_df,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            spark_session=spark_session,
            table_name=table_name,
        )
        query_context = offline_utils.get_feature_view_query_context(
            feature_refs=feature_refs,
            feature_views=feature_views,
            registry=registry,
            project=project,
            entity_df_timestamp_range=entity_df_timestamp_range,
        )
    else:
        # for feast<0.17
        query_context = offline_utils.get_feature_view_query_context(
            feature_refs=feature_refs,
            feature_views=feature_views,
            registry=registry,
            project=project,
        )
    return query_context


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE OR REPLACE TEMPORARY VIEW entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
);
---EOS---
-- Start create temporary table *__base
{% for featureview in featureviews %}
CREATE OR REPLACE TEMPORARY VIEW {{ featureview.name }}__base AS
WITH {{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}},
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ featureview.entities | join(', ')}}, entity_timestamp, {{featureview.name}}__entity_row_unique_id
),
/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.
 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously
 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/
{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_
               {% if featureview.ttl == 0 %}{% else %}
               ,(MIN(entity_timestamp) - interval '{{ featureview.ttl }}' second) as min_entity_timestamp_
               {% endif %}
        FROM entity_dataframe
    ) AS temp
    ON (
        {{ featureview.event_timestamp_column }} <= max_entity_timestamp_
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.event_timestamp_column }} >=  min_entity_timestamp_
        {% endif %}
    )
)
SELECT
    subquery.*,
    entity_dataframe.entity_timestamp,
    entity_dataframe.{{featureview.name}}__entity_row_unique_id
FROM {{ featureview.name }}__subquery AS subquery
INNER JOIN (
    SELECT *
    {% if featureview.ttl == 0 %}{% else %}
    , (entity_timestamp - interval '{{ featureview.ttl }}' second) as ttl_entity_timestamp
    {% endif %}
    FROM {{ featureview.name }}__entity_dataframe
) AS entity_dataframe
ON (
    subquery.event_timestamp <= entity_dataframe.entity_timestamp
    {% if featureview.ttl == 0 %}{% else %}
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    {% endif %}
    {% for entity in featureview.entities %}
    AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
    {% endfor %}
);

---EOS---

{% endfor %}
-- End create temporary table *__base


{% for featureview in featureviews %}
{% if loop.first %}WITH{% endif %}
/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}
/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        base.{{featureview.name}}__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,MAX(base.created_timestamp) AS created_timestamp
        {% endif %}
    FROM {{ featureview.name }}__base AS base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup AS dedup
        ON (
            dedup.{{featureview.name}}__entity_row_unique_id=base.{{featureview.name}}__entity_row_unique_id
            AND dedup.event_timestamp=base.event_timestamp
            AND dedup.created_timestamp=base.created_timestamp
        )
    {% endif %}
    GROUP BY base.{{featureview.name}}__entity_row_unique_id
),
/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base AS base
    INNER JOIN {{ featureview.name }}__latest AS latest
    ON (
        base.{{featureview.name}}__entity_row_unique_id=latest.{{featureview.name}}__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
        {% if featureview.created_timestamp_column %}
            AND base.created_timestamp=latest.created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}
{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */
SELECT `(entity_timestamp|{% for featureview in featureviews %}{{featureview.name}}__entity_row_unique_id{% if loop.last %}{% else %}|{% endif %}{% endfor %})?+.+`
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) AS {{ featureview.name }}__joined
ON (
    {{ featureview.name }}__joined.{{featureview.name}}__entity_row_unique_id=entity_dataframe.{{featureview.name}}__entity_row_unique_id
)
{% endfor %}"""

class SparkDataSourceCreator(DataSourceCreator):
    tables: List[str] = []
    spark_offline_store_config = None
    spark_session = None

    def __init__(self, project_name: str):
        self.spark_conf = {'master': 'local[*]', 'spark.ui.enabled': 'false', 'spark.eventLog.enabled': 'false', 'spark.sql.parser.quotedRegexColumnNames': 'true', 'spark.sql.session.timeZone': 'UTC'}
        self.project_name = project_name
        if not self.spark_offline_store_config:
            self.create_offline_store_config()
        if not self.spark_session:
            self.spark_session = (SparkSession
                .builder
                .config(conf=SparkConf().setAll(self.spark_conf.items()))
                .appName('pytest-pyspark-local-testing')
                .getOrCreate())
        self.tables: List[str] = []

    def teardown(self):
        self.spark_session.stop()

    def create_offline_store_config(self):
        self.spark_offline_store_config = SparkOfflineStoreConfig()
        self.spark_offline_store_config.type = "feast_spark_offline_store.spark.SparkOfflineStore"
        self.spark_offline_store_config.spark_conf = self.spark_conf
        return self.spark_offline_store_config

    # abstract
    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
        **kwargs,
    ) -> DataSource:
            #df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        if event_timestamp_column in df:
            df[event_timestamp_column] = pd.to_datetime(df[event_timestamp_column], utc=True)
        # Make sure the field mapping is correct and convert the datetime datasources.
        if field_mapping:
            timestamp_mapping = {value:key for key, value in field_mapping.items()}
            if(event_timestamp_column in timestamp_mapping and timestamp_mapping[event_timestamp_column] in df):
                col = timestamp_mapping[event_timestamp_column]
                df[col] = pd.to_datetime(df[col], utc=True)

        # https://stackoverflow.com/questions/51871200/analysisexception-it-is-not-allowed-to-add-database-prefix
        # destination_name = self.get_prefixed_table_name(destination_name)

        df = self.spark_session.createDataFrame(df).createOrReplaceTempView(destination_name)

        self.tables.append(destination_name)
        return SparkSource(
            table=destination_name,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            # feature_view => datasource accompanied
            # maps certain column names to other names
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetSparkStorage:
        table = f"persisted_{str(uuid.uuid4()).replace('-', '_')}"
        return SavedDatasetSparkStorage(table_ref=table, query="")

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}.{suffix}"


