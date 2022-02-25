from distutils.dir_util import copy_tree
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from feast import FeatureStore

ROOT = (Path(__file__).parent / "..").resolve()
EXAMPLE_REPO_PATH = ROOT / "example_feature_repo"


@pytest.fixture(scope="function")
def feature_store() -> FeatureStore:
    """Creates a feature store object whose data is cleaned up automatically"""
    with TemporaryDirectory(dir=ROOT) as tmp_dir:
        copy_tree(src=EXAMPLE_REPO_PATH, dst=tmp_dir)
        yield FeatureStore(repo_path=tmp_dir)
