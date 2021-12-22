# Feast Spark Offline Store plugin
This repo contains a plugin for [feast](https://github.com/feast-dev/feast) to run an offline store on Spark. 
It can be installed from pip and configured in the `feature_store.yaml` configuration file to interface with `DataSources` using Spark.

## Contributing
We strongly encourage you to contribute to our repository. Find out more in our [contribution guidelines](https://github.com/Adyen/.github/blob/master/CONTRIBUTING.md)

## Requirements
Currently only supports Feast version `0.14.1`.

## Installation
`pip install feast-spark-offline-store` 

Or to install from source:
```bash
git clone git@github.com:Adyen/feast-spark-offline-store.git
cd feast-spark-offline-store
pip install -e '.[dev]'
```

## Usage
Install `feast` and `feast_spark_offline_store` and change the Feast configurations in `feature_store.yaml` to use `feast_spark_offline_store.SparkOfflineStore`:

```yaml
project: example_feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: feast_spark_offline_store.spark.SparkOfflineStore
    spark_conf:
        spark.master: "local[*]"
        spark.ui.enabled: "false"
        spark.eventLog.enabled: "false"
        spark.sql.catalogImplementation: "hive"
        spark.sql.parser.quotedRegexColumnNames: "true"
        spark.sql.session.timeZone: "UTC"
```

## Documentation
See Feast documentation on [offline stores](https://docs.feast.dev/getting-started/architecture-and-components/offline-store) and [adding custom offline stores](https://docs.feast.dev/how-to-guides/adding-a-new-offline-store). 

## Support
If you have a feature request, or spotted a bug or a technical problem, create a GitHub issue. For other questions, contact our [support team](https://support.adyen.com/hc/en-us/requests/new?ticket_form_id=360000705420).    

## License    
MIT license. For more information, see the LICENSE file.