from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def provide_oci_config():
    return Path("test/test_config.yaml")
