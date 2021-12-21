from pkg_resources import DistributionNotFound, get_distribution

from .spark import SparkOfflineStoreConfig, SparkOfflineStore
from .spark_source import SparkOptions, SparkSource

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

__all__ = ["SparkOptions", "SparkSource", "SparkOfflineStoreConfig", "SparkOfflineStore"]
