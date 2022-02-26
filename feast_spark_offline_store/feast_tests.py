from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

import os
import sys

# os.environ['PYSPARK_PYTHON'] = sys.executable
from feast_spark_offline_store.spark import SparkDataSourceCreator
from pyspark.sql import SparkSession
from pyspark import SparkConf

# @pytest.fixture(scope="session")
# def spark_session(request):
#     """Fixture for creating a spark context."""
#     spark_conf = {'spark.ui.enabled': 'false', 'spark.eventLog.enabled': 'false','spark.sql.parser.quotedRegexColumnNames': 'true', 'spark.sql.session.timeZone': 'UTC'}

#     spark = (SparkSession
#              .builder
#              .master('local[*]')
#              .appName('pytest-pyspark-local-testing')
#              .enableHiveSupport()
#              .config(conf = SparkConf().setAll(spark_conf.items()))
#              .getOrCreate())
#     request.addfinalizer(lambda: spark.stop())
#     return spark

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=SparkDataSourceCreator,
    )
]