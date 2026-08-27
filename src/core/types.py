"""Shared base types used across every pipeline stage.

StageResult is the common base every stage-specific result (IngestionResult,
SchemaValidationResult, etc.) extends, so cross-cutting code (logging,
monitoring, pipeline.py's own bookkeeping) can treat any stage's outcome
uniformly without needing to know which stage produced it.

DatasetRef is the common handle stages pass to each other for data they
produce, carrying enough cheap, independently-checkable facts (row count,
a schema fingerprint) that a downstream stage can sanity-check what it
received before doing real work with it.
"""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class StageStatus(str, Enum):
    """Terminal status of a pipeline stage run."""

    SUCCESS = "success"
    FAILED = "failed"


def _now(clock: Callable[[], datetime] | None = None) -> str:
    """Current UTC time as an ISO 8601 string.

    Mirrors IngestionManifest's own clock convention (see manifest.py)
    so timestamps are formatted consistently everywhere in the pipeline,
    and so callers can inject a fake clock in tests instead of asserting
    against real wall-clock time.
    """
    active_clock = clock or (lambda: datetime.now(timezone.utc))
    return active_clock().isoformat()


@dataclass
class StageResult:
    """Common outcome fields every stage-specific result extends.

    Subclasses add their own fields (e.g. IngestionResult adds
    `succeeded`/`failed` item lists) but every stage's result shares
    this shape, so generic code — logging, monitoring, a future CI/CD
    dashboard — can report on any stage without stage-specific logic.
    """

    stage_name: str
    status: StageStatus
    started_at: str
    completed_at: str
    duration_seconds: float

    @classmethod
    def start(cls, stage_name: str, clock: Callable[[], datetime] | None = None) -> "_OpenStage":
        """Begins timing a stage run.

        Returns an _OpenStage handle — call .finish(**fields) on it once
        the stage completes (success or failure) to produce the actual
        result instance, with completed_at/duration_seconds/status filled
        in consistently. This keeps every stage from re-implementing the
        same "compute elapsed time, set status" logic independently.
        """
        return _OpenStage(stage_name=stage_name, clock=clock, started_at_dt=(clock or (lambda: datetime.now(timezone.utc)))())


@dataclass
class _OpenStage:
    """Internal helper tracking an in-progress stage's start time.

    Not meant to be constructed directly — use StageResult.start(...).
    """

    stage_name: str
    started_at_dt: datetime
    clock: Callable[[], datetime] | None = None

    def finish(self, result_cls: type, status: StageStatus, **fields) -> "StageResult":
        """Finalizes a stage run, producing a populated result instance.

        Args:
            result_cls: The StageResult subclass to construct (e.g.
                IngestionResult). Must accept stage_name/status/
                started_at/completed_at/duration_seconds plus whatever
                stage-specific fields are passed via **fields.
            status: StageStatus.SUCCESS or StageStatus.FAILED.
            **fields: The stage-specific fields for result_cls (e.g.
                succeeded=..., failed=... for IngestionResult).

        Returns:
            A populated instance of result_cls, ready to return (on
            success) or attach to a raised stage-failure exception (on
            failure) — call this at both exit points so timing/status
            logic isn't duplicated at each one.
        """
        completed_at_dt = (self.clock or (lambda: datetime.now(timezone.utc)))()
        duration_seconds = (completed_at_dt - self.started_at_dt).total_seconds()

        return result_cls(
            stage_name=self.stage_name,
            status=status,
            started_at=self.started_at_dt.isoformat(),
            completed_at=completed_at_dt.isoformat(),
            duration_seconds=duration_seconds,
            **fields,
        )


@dataclass
class DatasetRef:
    """A lightweight, independently-checkable handle to a dataset a
    stage produced, passed forward to whichever stage runs next.

    Carrying row_count and schema_hash alongside the path lets a
    downstream stage sanity-check what it received (e.g. reject a
    zero-row dataset, or a schema fingerprint that doesn't match what
    it was told to expect) without re-reading the whole file just to
    find out something upstream silently went wrong.
    """

    path: Path
    row_count: int
    schema_hash: str
    created_at: str


def _read_header_and_count_csv(path: Path) -> tuple[list[str], int]:
    """Returns (column_names, row_count) for a CSV file, counting rows
    by streaming rather than loading the whole file into memory."""
    with open(path, mode="r", encoding="utf-8", newline="") as f:
        import csv

        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return [], 0
        row_count = sum(1 for _ in reader)
        return header, row_count


def _read_header_and_count_parquet(path: Path) -> tuple[list[str], int]:
    """Returns (column_names, row_count) for a Parquet file using
    pyarrow's metadata, which avoids loading actual row data into
    memory just to count rows or read column names."""
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    schema = parquet_file.schema_arrow
    return list(schema.names), parquet_file.metadata.num_rows


def compute_schema_hash(column_names: list[str]) -> str:
    """Fingerprints a dataset's schema as a stable SHA-256 hex digest.

    Column names are sorted before hashing so two datasets with the
    same columns in a different order still produce the same hash —
    schema identity shouldn't depend on column order.
    """
    canonical = json.dumps(sorted(column_names))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_dataset_ref(path: Path, clock: Callable[[], datetime] | None = None) -> DatasetRef:
    """Builds a DatasetRef for a dataset a stage just produced.

    Supports .csv and .parquet based on the file's suffix. Any stage
    producing a dataset for a downstream stage should build its
    DatasetRef through this function rather than computing row_count/
    schema_hash independently, so every stage fingerprints schemas the
    same way and comparisons between them stay meaningful.

    Raises:
        ValueError: if path's extension isn't a supported format.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        column_names, row_count = _read_header_and_count_csv(path)
    elif suffix == ".parquet":
        column_names, row_count = _read_header_and_count_parquet(path)
    else:
        raise ValueError(
            f"Unsupported dataset format {suffix!r} for {path}. "
            "build_dataset_ref currently supports .csv and .parquet."
        )

    return DatasetRef(
        path=path,
        row_count=row_count,
        schema_hash=compute_schema_hash(column_names),
        created_at=_now(clock),
    )