from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


SCRIPTS = Path(__file__).parents[1] / "spark-apps/customer_etl/scripts"
sys.path.insert(0, str(SCRIPTS))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("customer-etl-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
