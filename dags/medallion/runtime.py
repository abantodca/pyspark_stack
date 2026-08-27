from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Protocol
from urllib.parse import urlparse

DEFAULT_HDFS_ROOT = "hdfs://hdfs-namenode:9000/lakehouse"
SUPPORTED_LAYERS = frozenset({"bronze", "silver", "gold", "quality", "quarantine"})
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


@dataclass(frozen=True)
class LakehouseConfig:
    """Validated physical configuration for one data product."""

    project: str
    root: str = field(
        default_factory=lambda: os.getenv("LAKEHOUSE_ROOT", DEFAULT_HDFS_ROOT)
    )

    def __post_init__(self) -> None:
        if not SAFE_NAME.fullmatch(self.project):
            raise ValueError(f"Invalid medallion project name: {self.project!r}")

        parsed = urlparse(self.root)
        allow_test_storage = os.getenv("MEDALLION_ALLOW_NON_HDFS_FOR_TESTS") == "true"
        if parsed.scheme != "hdfs" and not allow_test_storage:
            raise ValueError(
                "LAKEHOUSE_ROOT must use hdfs://; set "
                "MEDALLION_ALLOW_NON_HDFS_FOR_TESTS=true only in isolated tests"
            )
        if parsed.scheme == "hdfs" and not parsed.netloc:
            raise ValueError("LAKEHOUSE_ROOT must include the HDFS namenode authority")

        object.__setattr__(self, "root", self.root.rstrip("/"))

    @property
    def filesystem_uri(self) -> str:
        parsed = urlparse(self.root)
        if parsed.scheme == "hdfs":
            return f"hdfs://{parsed.netloc}"
        return "file:///"

    def location(self, layer: str, run_date: str, dataset: str | None = None) -> str:
        if layer not in SUPPORTED_LAYERS:
            raise ValueError(f"Unsupported medallion layer: {layer!r}")
        date.fromisoformat(run_date)
        if dataset is not None and not SAFE_NAME.fullmatch(dataset):
            raise ValueError(f"Invalid dataset name: {dataset!r}")

        suffix = f"/{dataset}" if dataset else ""
        return f"{self.root}/{layer}/{self.project}/run_date={run_date}{suffix}"


class SparkSessionFactory:
    """Creates consistently configured Spark drivers for a project."""

    def __init__(self, config: LakehouseConfig) -> None:
        self._config = config

    def create(self, stage: str):
        if not SAFE_NAME.fullmatch(stage):
            raise ValueError(f"Invalid Spark stage name: {stage!r}")

        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.appName(f"{self._config.project}-{stage}")
            .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
            .config("spark.hadoop.fs.defaultFS", self._config.filesystem_uri)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.compression.codec", "snappy")
        )
        # A standalone Spark driver serializes the Python executable name with
        # each job. Make the executor contract explicit when the deployment
        # defines it, while keeping local unit tests independent of a path such
        # as `python3.14`.
        executor_python = os.getenv("PYSPARK_PYTHON")
        if executor_python:
            builder = builder.config("spark.pyspark.python", executor_python)

        return builder.getOrCreate()


class HdfsLakehouseStorage:
    """Single gateway for every DataFrame persisted by the DAGs."""

    def __init__(self, config: LakehouseConfig) -> None:
        self._config = config

    def path(self, layer: str, run_date: str, dataset: str | None = None) -> str:
        return self._config.location(layer, run_date, dataset)

    @property
    def project(self) -> str:
        return self._config.project

    def write(
        self, frame, layer: str, run_date: str, dataset: str | None = None
    ) -> None:
        destination = self.path(layer, run_date, dataset)
        (
            frame.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(destination)
        )


@dataclass(frozen=True)
class QualityCounts:
    received: int
    rejected: int
    published: int

    @property
    def rejected_ratio(self) -> float:
        return self.rejected / self.received if self.received else 1.0


class QualityGate:
    """Publishes auditable counts and stops batches outside their data SLO."""

    def __init__(self, storage: DataFrameStorage) -> None:
        self._storage = storage

    def publish_and_validate(
        self,
        spark,
        run_date: str,
        *,
        received: int,
        rejected: int,
        published: int,
        max_rejected_ratio: float,
    ) -> None:
        if not 0 <= max_rejected_ratio <= 1:
            raise ValueError("max_rejected_ratio must be between 0 and 1")

        counts = QualityCounts(received, rejected, published)
        metrics = spark.createDataFrame(
            [
                (
                    run_date,
                    counts.received,
                    counts.rejected,
                    counts.published,
                    counts.rejected_ratio,
                    max_rejected_ratio,
                )
            ],
            [
                "run_date",
                "received",
                "rejected",
                "published",
                "rejected_ratio",
                "max_rejected_ratio",
            ],
        )
        self._storage.write(metrics, "quality", run_date)

        if (
            counts.received == 0
            or counts.published == 0
            or counts.rejected_ratio > max_rejected_ratio
        ):
            raise ValueError(
                f"{self._storage.project} quality gate failed: "
                f"received={counts.received}, rejected={counts.rejected}, "
                f"published={counts.published}, rejected_ratio={counts.rejected_ratio:.2%}"
            )


class SessionProvider(Protocol):
    """Minimal dependency required by transformations that need Spark."""

    def create(self, stage: str): ...


class DataFrameStorage(Protocol):
    """Storage port; HDFS is the production adapter used by this stack."""

    @property
    def project(self) -> str: ...

    def path(self, layer: str, run_date: str, dataset: str | None = None) -> str: ...

    def write(
        self, frame, layer: str, run_date: str, dataset: str | None = None
    ) -> None: ...


class QualityPolicy(Protocol):
    """Quality port kept independent from Airflow and business transformations."""

    def publish_and_validate(
        self,
        spark,
        run_date: str,
        *,
        received: int,
        rejected: int,
        published: int,
        max_rejected_ratio: float,
    ) -> None: ...

class MedallionRuntime:
    """Facade composed from focused services; transformations depend on this API."""

    def __init__(
        self,
        project: str,
        *,
        sessions: SessionProvider | None = None,
        storage: DataFrameStorage | None = None,
        quality: QualityPolicy | None = None,
    ) -> None:
        config = LakehouseConfig(project)
        self._sessions = sessions or SparkSessionFactory(config)
        self._storage = storage or HdfsLakehouseStorage(config)
        self._quality = quality or QualityGate(self._storage)

    def spark(self, stage: str):
        return self._sessions.create(stage)

    def path(self, layer: str, run_date: str, dataset: str | None = None) -> str:
        return self._storage.path(layer, run_date, dataset)

    def write(
        self, frame, layer: str, run_date: str, dataset: str | None = None
    ) -> None:
        self._storage.write(frame, layer, run_date, dataset)

    def enforce_quality(
        self,
        spark,
        run_date: str,
        *,
        received: int,
        rejected: int,
        published: int,
        max_rejected_ratio: float,
    ) -> None:
        self._quality.publish_and_validate(
            spark,
            run_date,
            received=received,
            rejected=rejected,
            published=published,
            max_rejected_ratio=max_rejected_ratio,
        )