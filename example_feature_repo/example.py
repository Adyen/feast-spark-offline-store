# # # # # # # # # # # # # # # # # # # # # # # #
# This is an example feature definition file  #
# # # # # # # # # # # # # # # # # # # # # # # #

from datetime import datetime, timedelta
from pathlib import Path

from feast import Entity, Feature, FeatureView, ValueType
from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_customer_daily_profile_df,
)
from google.protobuf.duration_pb2 import Duration
from pyspark.sql import SparkSession

from feast_spark_offline_store import SparkSource

# Constants related to the generated data sets
current_dir = Path(__file__).parent
start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
end_date = start_date + timedelta(days=7)
driver_entities = [1001, 1002, 1003]
customer_entities = [201, 202, 203]

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

# Sources
driver_hourly_stats = SparkSource(
    path=f"{current_dir}/data/driver_hourly_stats",
    file_format="parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)
customer_daily_profile = SparkSource(
    path=f"{current_dir}/data/customer_daily_profile",
    file_format="parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
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
    batch_source=customer_daily_profile,
    tags={},
)


def generate_example_data(spark_session: SparkSession, base_dir: str) -> None:
    spark_session.createDataFrame(
        data=create_driver_hourly_stats_df(driver_entities, start_date, end_date)
    ).write.parquet(
        path=str(Path(base_dir) / "data" / "driver_hourly_stats"),
        mode="overwrite",
    )

    spark_session.createDataFrame(
        data=create_customer_daily_profile_df(customer_entities, start_date, end_date)
    ).write.parquet(
        path=str(Path(base_dir) / "data" / "customer_daily_profile"),
        mode="overwrite",
    )


if __name__ == "__main__":
    generate_example_data(
        spark_session=SparkSession.builder.getOrCreate(),
        base_dir=str(current_dir),
    )
