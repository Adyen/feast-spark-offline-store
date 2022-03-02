import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from _pytest.tmpdir import TempPathFactory
from feast import FeatureStore
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.repo_config import load_repo_config, RepoConfig
from filelock import FileLock
from pyspark.sql import SparkSession, DataFrame

from example_feature_repo.example import generate_example_data
from feast_spark_offline_store.spark import (
    get_spark_session_or_start_new_with_repoconfig,
)


@pytest.fixture(scope="session")
def root_path() -> str:
    """Fixture that returns the root path of this git repository"""
    return str((Path(__file__).parent / "..").resolve())


@pytest.fixture(scope="session")
def example_repo_path(root_path: str) -> str:
    """Fixture that returns the path of the example feature repository"""
    return str(Path(root_path) / "example_feature_repo")


@pytest.fixture(scope="function")
def tmp_dir(root_path: str) -> TemporaryDirectory:
    """Fixture that returns a temp dir, which is cleaned up automatically after tests"""
    with TemporaryDirectory(dir=root_path) as tmp_dir:
        yield tmp_dir


@pytest.fixture(scope="function")
def repo_config(tmp_dir: TemporaryDirectory, example_repo_path: str) -> RepoConfig:
    """
    Fixture that returns a repo config to be used for testing

    Config is based on example_feature_repo/feature_store.yaml, but with two important
    changes:
     - registry store (sqllite) points to a tmp dir
     - online store (sqllite) points to the same tmp dir

    If we don't do this, all tests will run agains the same registry/online store
    instances, which is not possible with parallel test execution.
    """
    repo_config = load_repo_config(repo_path=Path(example_repo_path))
    repo_config.registry = str(Path(tmp_dir) / "registry.db")
    repo_config.online_store.path = str(Path(tmp_dir) / "online_store.db")
    return repo_config


@pytest.fixture(scope="session")
def spark_session(example_repo_path: str) -> SparkSession:
    """Creates a spark session based on the repo configs in the example repo path"""
    repo_config = load_repo_config(repo_path=Path(example_repo_path))
    return get_spark_session_or_start_new_with_repoconfig(
        store_config=repo_config.offline_store,
    )


@pytest.fixture(scope="session")
def test_data(
    spark_session: SparkSession,
    example_repo_path: str,
    tmp_path_factory: TempPathFactory,
    worker_id: str,
) -> None:
    """
    Fixture that ensures test data is generated and written to example_feature_repo/data

    Test data is not included in the repo, and thus needs to be generated before tests
    can be run. This is ensured via this pytest fixture.

    This fixture contains logic that prevents writing test data twice if running the
    tests in parallel mode. For reference, see:

    https://pypi.org/project/pytest-xdist/#making-session-scoped-fixtures-execute-only-once
    """
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        generate_example_data(
            spark_session=spark_session,
            base_dir=example_repo_path
        )
        return

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            pass
        else:
            generate_example_data(
                spark_session=spark_session,
                base_dir=example_repo_path
            )
    return


@pytest.fixture(scope="function")
def feature_store(repo_config: RepoConfig, example_repo_path: str) -> FeatureStore:
    """Fixture that returns a feature store instance that can be used in parallel tests"""
    feature_store = FeatureStore(config=repo_config)
    feature_store.repo_path = str(example_repo_path)
    return feature_store


@pytest.fixture(scope="session")
def example_df(spark_session: SparkSession) -> DataFrame:
    """Fixture that returns a test dataframe"""
    driver_entities = [1001, 1002, 1003, 1004, 1005]
    end_date = datetime.datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - datetime.timedelta(days=3)
    return spark_session.createDataFrame(
        data=create_driver_hourly_stats_df(driver_entities, start_date, end_date)
    )
