import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("dtb-iop-raw-elt-tests").getOrCreate()
    yield session
    session.stop()
