# Feast Spark Offline Store

Custom offline store for feast to interface with spark.

Currently only support for feast `0.14.1`

## Installation

`pip install -e .` 

Development:

`pip install -e .[dev]` 

Testing:

`pip install -e .[test]`

## Use with feast
Install feast and `feast_spark_offline_store` and change feast configurations in `feature_store.yaml` or 
`RepoConfig` to use `feast_spark_offline_store.SparkOfflineStore`