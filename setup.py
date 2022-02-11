from distutils.core import setup

from setuptools import find_packages

INSTALL_REQUIRES = [
    "feast==0.14.1",
    "pyspark>=3.0",
    "pyarrow>=1.0.0",
    "numpy",
    "pandas",
    "pytz>=2021.3",
    "pydantic>=1.6",
]

DEV_REQUIRES = INSTALL_REQUIRES + [
    "wheel",
    "black",
    "flake8",
    "pytest>=6.2.5",
    "google",
]

setup(
    name="feast_spark_offline_store",
    version="0.0.4",
    author="Thijs Brits",
    description="Spark support for Feast offline store",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Adyen/feast-spark-offline-store",
    license="MIT",
    python_requires=">=3.8.0",
    packages=find_packages(include=["feast_spark_offline_store"]),
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIRES},
)
