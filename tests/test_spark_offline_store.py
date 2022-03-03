import os

import pandas as pd
from feast import FeatureStore

from example_feature_repo.example import (
    driver,
    driver_hourly_stats_view,
    customer,
    customer_daily_profile_view,
    end_date,
    driver_entities,
    customer_entities,
)


def test_end_to_end_one_feature_view(feature_store: FeatureStore, test_data):
    try:
        # apply repository
        feature_store.apply([driver, driver_hourly_stats_view])

        # load data into online store (uses offline stores pull_latest_from_table_or_query)
        feature_store.materialize_incremental(end_date=end_date)

        entity_df = pd.DataFrame(
            {"driver_id": [driver_entities[0]], "event_timestamp": [end_date]}
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


def test_end_to_end_multiple_feature_views(feature_store: FeatureStore, test_data):
    try:
        # apply repository
        feature_store.apply(
            [driver, driver_hourly_stats_view, customer, customer_daily_profile_view]
        )

        # load data into online store (uses offline stores pull_latest_from_table_or_query)
        feature_store.materialize_incremental(end_date=end_date)

        entity_df = pd.DataFrame(
            {
                "driver_id": [driver_entities[0]],
                "customer_id": [customer_entities[0]],
                "event_timestamp": [end_date],
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


def test_cli(example_repo_path: str, test_data):
    repo_path = example_repo_path
    output_file = f"{repo_path}/output.txt"
    try:
        # Run apply
        os.system(f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast --chdir {repo_path} apply")

        # Run materialize while piping stdout to a file
        timestamp = end_date.strftime("%Y-%m-%dT%H:%M:%S")  # needs to be ISO format
        os.system(
            f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast --chdir {repo_path} "
            f"materialize-incremental {timestamp} > {output_file}"
        )

        # Verify success via presence of expected logs in stdout
        with open(output_file, "r", encoding="utf-8") as f:
            output = f.read()
        if "Pulling latest features from spark offline store" not in output:
            raise Exception(
                "Failed to successfully use provider from CLI. Contents of "
                f"'{output_file}':\n\n{output}"
            )
    finally:
        os.remove(output_file)
        os.system(f"PYTHONPATH=$PYTHONPATH:/$(pwd) feast --chdir {repo_path} teardown")
