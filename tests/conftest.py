import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pyspark.sql import SparkSession, DataFrame

from feast import FeatureStore
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.repo_config import load_repo_config, RepoConfig
from feast_spark_offline_store.spark import (
    get_spark_session_or_start_new_with_repoconfig,
)


@pytest.fixture(scope="session")
def root_path() -> str:
    return str((Path(__file__).parent / "..").resolve())


@pytest.fixture(scope="session")
def example_repo_path(root_path: str) -> str:
    return str(Path(root_path) / "example_feature_repo")


@pytest.fixture(scope="function")
def tmp_dir(root_path: str) -> TemporaryDirectory:
    with TemporaryDirectory(dir=root_path) as tmp_dir:
        yield tmp_dir


@pytest.fixture(scope="function")
def repo_config(tmp_dir: TemporaryDirectory, example_repo_path: str) -> RepoConfig:
    repo_config = load_repo_config(repo_path=Path(example_repo_path))
    repo_config.registry = str(Path(tmp_dir) / "registry.db")
    repo_config.online_store.path = str(Path(tmp_dir) / "online_store.db")
    return repo_config


@pytest.fixture(scope="function")
def spark_session(repo_config: RepoConfig) -> SparkSession:
    """Creates a spark session based on the repo configs in the example repo path"""
    return get_spark_session_or_start_new_with_repoconfig(
        store_config=repo_config.offline_store,
    )


@pytest.fixture(scope="function")
def feature_store(repo_config: RepoConfig, example_repo_path: str) -> FeatureStore:
    """Creates a feature store object whose data is cleaned up automatically"""
    feature_store = FeatureStore(config=repo_config)
    feature_store.repo_path = str(example_repo_path)
    return feature_store


@pytest.fixture(scope="function")
def example_df(spark_session: SparkSession) -> DataFrame:
    driver_entities = [1001, 1002, 1003, 1004, 1005]
    end_date = datetime.datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - datetime.timedelta(days=3)
    return spark_session.createDataFrame(
        data=create_driver_hourly_stats_df(driver_entities, start_date, end_date)
    )
