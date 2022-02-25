import os
import pandas as pd
from datetime import datetime

from feast import FeatureStore

from example_feature_repo.example import (
    driver,
    driver_hourly_stats_view,
    customer,
    customer_daily_profile_view,
)


def test_end_to_end_one_feature_view(feature_store: FeatureStore):
    try:
        # apply repository
        feature_store.apply([driver, driver_hourly_stats_view])

        # load data into online store (uses offline stores pull_latest_from_table_or_query)
        feature_store.materialize_incremental(end_date=datetime.now())

        entity_df = pd.DataFrame(
            {"driver_id": [1001], "event_timestamp": [datetime.now()]}
        )

        # Read features from offline store
        feature_vector = (
            feature_store.get_historical_features(
                features=["driver_hourly_stats:conv_rate"], entity_df=entity_df
            )
            .to_df()
            .to_dict()
        )
        conv_rate = feature_vector["conv_rate"][0]
        assert conv_rate > 0
    finally:
        # tear down feature store
        feature_store.teardown()


def test_end_to_end_multiple_feature_views(feature_store: FeatureStore):
    try:
        # apply repository
        feature_store.apply(
            [driver, driver_hourly_stats_view, customer, customer_daily_profile_view]
        )

        # load data into online store (uses offline stores pull_latest_from_table_or_query)
        feature_store.materialize_incremental(end_date=datetime.now())

        entity_df = pd.DataFrame(
            {
                "driver_id": [1001],
                "customer_id": [201],
                "event_timestamp": [datetime.now()],
            }
        )

        # Read features from offline store
        feature_vector = (
            feature_store.get_historical_features(
                features=[
                    "driver_hourly_stats:conv_rate",
                    "customer_daily_profile:lifetime_trip_count",
                ],
                entity_df=entity_df,
            )
            .to_df()
            .to_dict()
        )
        conv_rate = feature_vector["conv_rate"][0]
        assert conv_rate > 0

        lifetime_trip_count = feature_vector["lifetime_trip_count"][0]
        assert lifetime_trip_count > 0

    finally:
        # tear down feature store
        feature_store.teardown()


def test_cli(feature_store: FeatureStore):
    repo_name = feature_store.repo_path
    os.system(f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c {repo_name} apply")
    try:
        os.system(
            f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c {repo_name} "
            f"materialize-incremental 2021-08-19T22:29:28 > {repo_name}/output"
        )

        with open(f"{repo_name}/output", "r") as f:
            output = f.read()

        if "Pulling latest features from spark offline store" not in output:
            raise Exception(
                'Failed to successfully use provider from CLI. See "output" for more details.'
            )
    finally:
        os.system(f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c {repo_name} teardown")
