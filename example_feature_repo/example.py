# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast_spark_offline_store import SparkSource
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_customer_daily_profile_df,
)

# # we are loading a sparksession here, but should be configurable in the yaml
spark = SparkSession.builder.getOrCreate()

end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
start_date = end_date - timedelta(days=15)

driver_entities = [1001, 1002, 1003, 1004, 1005]
driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)


# this would just be a registered table
spark.createDataFrame(driver_df).createOrReplaceTempView("driver_stats")
# ####

driver_hourly_stats = SparkSource(
    table="driver_stats",  # must be serializable so no support of DataFrame objects
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver.
driver = Entity(
    name="driver_id",
    value_type=ValueType.INT64,
    description="driver id",
)

# Define FeatureView
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)


customer_entities = [201, 202, 203, 204, 205]
customer_df = create_customer_daily_profile_df(customer_entities, start_date, end_date)
spark.createDataFrame(customer_df).createOrReplaceTempView("customer_daily_stats")

customer_daily_stats = SparkSource(
    table="customer_daily_stats",  # must be serializable so no support of DataFrame objects
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

customer = Entity(
    name="customer_id",
    value_type=ValueType.INT64,
    description="customer id",
)

# Define FeatureView
customer_daily_profile_view = FeatureView(
    name="customer_daily_profile",
    entities=["customer_id"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="current_balance", dtype=ValueType.FLOAT),
        Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
        Feature(name="lifetime_trip_count", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=customer_daily_stats,
    tags={},
)
