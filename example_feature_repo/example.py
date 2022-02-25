# # # # # # # # # # # # # # # # # # # # # # # #
#  This is an example feature definition file #
# # # # # # # # # # # # # # # # # # # # # # # #

from datetime import datetime, timedelta

from feast import Entity, Feature, FeatureView, ValueType
from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_customer_daily_profile_df,
)
from google.protobuf.duration_pb2 import Duration
from pyspark.sql import SparkSession

from feast_spark_offline_store import SparkSource

# We are loading a spark session here, but should be configurable in the yaml
spark = SparkSession.builder.getOrCreate()

# Create some temp tables, normally these would registered hive tables
end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
start_date = end_date - timedelta(days=15)

driver_entities = [1001, 1002, 1003, 1004, 1005]
driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
spark.createDataFrame(driver_df).createOrReplaceTempView("driver_stats")

customer_entities = [201, 202, 203, 204, 205]
customer_df = create_customer_daily_profile_df(customer_entities, start_date, end_date)
spark.createDataFrame(customer_df).createOrReplaceTempView("customer_daily_stats")

# Next we can define the spark sources, the table name has to match the tmp views
driver_hourly_stats = SparkSource(
    table="driver_stats",  # must be serializable so no support of DataFrame objects
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)
customer_daily_stats = SparkSource(
    table="customer_daily_stats",  # must be serializable so no support of DataFrame objects
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Entity definitions
driver = Entity(
    name="driver_id",
    value_type=ValueType.INT64,
    description="driver id",
)
customer = Entity(
    name="customer_id",
    value_type=ValueType.INT64,
    description="customer id",
)

# Finally the feature views link everything together
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 7),  # one week
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)
customer_daily_profile_view = FeatureView(
    name="customer_daily_profile",
    entities=["customer_id"],
    ttl=Duration(seconds=86400 * 7),  # one week
    features=[
        Feature(name="current_balance", dtype=ValueType.FLOAT),
        Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
        Feature(name="lifetime_trip_count", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=customer_daily_stats,
    tags={},
)
