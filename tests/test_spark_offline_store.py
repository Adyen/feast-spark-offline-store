import os
import pandas as pd
from datetime import datetime

from feast import FeatureStore

from example_feature_repo.example import driver, driver_hourly_stats_view, customer, customer_daily_profile_view


def test_end_to_end_one_feature_view():
    fs = FeatureStore("example_feature_repo/")

    try:
        # apply repository
        fs.apply([driver, driver_hourly_stats_view])

        # load data into online store (uses offline stores pull_latest_from_table_or_query)
        fs.materialize_incremental(end_date=datetime.now())

        entity_df = pd.DataFrame(
            {"driver_id": [1001], "event_timestamp": [datetime.now()]}
        )

        # Read features from offline store
        feature_vector = (
            fs.get_historical_features(
                features=["driver_hourly_stats:conv_rate"], entity_df=entity_df
            )
            .to_df()
            .to_dict()
        )
        conv_rate = feature_vector["conv_rate"][0]
        assert conv_rate > 0
    finally:
        # tear down feature store
        fs.teardown()


def test_end_to_end_multiple_feature_views():
    fs = FeatureStore("example_feature_repo/")

    try:
        # apply repository
        fs.apply([driver, driver_hourly_stats_view, customer, customer_daily_profile_view])

        # load data into online store (uses offline stores pull_latest_from_table_or_query)
        fs.materialize_incremental(end_date=datetime.now())

        entity_df = pd.DataFrame(
            {"driver_id": [1001], "customer_id": [201], "event_timestamp": [datetime.now()]}
        )

        # Read features from offline store
        feature_vector = (
            fs.get_historical_features(
                features=["driver_hourly_stats:conv_rate", "customer_daily_profile:lifetime_trip_count"],
                entity_df=entity_df
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
        fs.teardown()


def test_cli():
    repo_name = "example_feature_repo"
    os.system(f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c {repo_name} apply")
    try:
        os.system(
            f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c {repo_name} materialize-incremental 2021-08-19T22:29:28 > output"
        )

        with open("output", "r") as f:
            output = f.read()

        if "Pulling latest features from spark offline store" not in output:
            raise Exception(
                'Failed to successfully use provider from CLI. See "output" for more details.'
            )
    finally:
        os.system(f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c {repo_name} teardown")
