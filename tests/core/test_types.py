"""Tests for src.core.types: StageResult, DatasetRef, and their helpers."""

import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.core.types import (
    DatasetRef,
    StageResult,
    StageStatus,
    build_dataset_ref,
    compute_schema_hash,
)


# ─────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────

@dataclass
class FakeStageResult(StageResult):
    """A minimal StageResult subclass, standing in for a real stage's
    result (e.g. IngestionResult) so these tests don't depend on any
    particular stage's own fields."""

    items_processed: int = 0


def fake_clock(times: list[datetime]):
    """Returns a zero-arg callable that yields each datetime in `times`
    in order, one per call — lets a test control exactly what
    started_at/completed_at come out to, instead of asserting against
    real wall-clock time."""
    iterator = iter(times)
    return lambda: next(iterator)


def write_csv(path: Path, columns: list[str], rows: list[list[str]]) -> None:
    with open(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


# ─────────────────────────────────────────────────────────────────
# StageResult.start / _OpenStage.finish
# ─────────────────────────────────────────────────────────────────

class TestStageResultTiming:

    def test_finish_populates_stage_name_and_status(self):
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        clock = fake_clock([t0, t1])

        open_stage = StageResult.start("ingestion", clock=clock)
        result = open_stage.finish(FakeStageResult, StageStatus.SUCCESS, items_processed=3)

        assert result.stage_name == "ingestion"
        assert result.status == StageStatus.SUCCESS
        assert result.items_processed == 3

    def test_finish_computes_duration_from_injected_clock(self):
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(seconds=7.5)
        clock = fake_clock([t0, t1])

        open_stage = StageResult.start("ingestion", clock=clock)
        result = open_stage.finish(FakeStageResult, StageStatus.SUCCESS)

        assert result.duration_seconds == pytest.approx(7.5)

    def test_completed_at_is_after_started_at(self):
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(seconds=1)
        clock = fake_clock([t0, t1])

        open_stage = StageResult.start("ingestion", clock=clock)
        result = open_stage.finish(FakeStageResult, StageStatus.SUCCESS)

        assert datetime.fromisoformat(result.completed_at) > datetime.fromisoformat(result.started_at)

    def test_finish_can_be_called_with_failed_status(self):
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
        clock = fake_clock([t0, t1])

        open_stage = StageResult.start("ingestion", clock=clock)
        result = open_stage.finish(FakeStageResult, StageStatus.FAILED, items_processed=1)

        assert result.status == StageStatus.FAILED
        assert result.duration_seconds >= 0

    def test_start_without_injected_clock_uses_real_time(self):
        # no fake clock — just confirms the default path doesn't error
        # and produces a sane, non-negative duration
        open_stage = StageResult.start("ingestion")
        result = open_stage.finish(FakeStageResult, StageStatus.SUCCESS)

        assert result.duration_seconds >= 0
        assert result.stage_name == "ingestion"


# ─────────────────────────────────────────────────────────────────
# compute_schema_hash
# ─────────────────────────────────────────────────────────────────

class TestComputeSchemaHash:

    def test_same_columns_same_order_produce_same_hash(self):
        assert compute_schema_hash(["a", "b", "c"]) == compute_schema_hash(["a", "b", "c"])

    def test_same_columns_different_order_produce_same_hash(self):
        assert compute_schema_hash(["a", "b", "c"]) == compute_schema_hash(["c", "a", "b"])

    def test_different_columns_produce_different_hash(self):
        assert compute_schema_hash(["a", "b", "c"]) != compute_schema_hash(["a", "b", "d"])

    def test_empty_columns_does_not_raise(self):
        result = compute_schema_hash([])
        assert isinstance(result, str)
        assert len(result) == 64  # sha256 hex digest length


# ─────────────────────────────────────────────────────────────────
# build_dataset_ref — CSV
# ─────────────────────────────────────────────────────────────────

class TestBuildDatasetRefCsv:

    def test_counts_rows_excluding_header(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, columns=["id", "value"], rows=[["1", "a"], ["2", "b"], ["3", "c"]])

        ref = build_dataset_ref(path)

        assert ref.row_count == 3

    def test_schema_hash_matches_compute_schema_hash_of_columns(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, columns=["id", "value"], rows=[["1", "a"]])

        ref = build_dataset_ref(path)

        assert ref.schema_hash == compute_schema_hash(["id", "value"])

    def test_empty_csv_with_only_header_has_zero_rows(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, columns=["id", "value"], rows=[])

        ref = build_dataset_ref(path)

        assert ref.row_count == 0

    def test_completely_empty_file_has_zero_rows_and_no_columns(self, tmp_path):
        path = tmp_path / "data.csv"
        path.write_text("")

        ref = build_dataset_ref(path)

        assert ref.row_count == 0
        assert ref.schema_hash == compute_schema_hash([])

    def test_path_is_preserved_on_the_ref(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, columns=["id"], rows=[["1"]])

        ref = build_dataset_ref(path)

        assert ref.path == path

    def test_created_at_uses_injected_clock(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, columns=["id"], rows=[["1"]])
        t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        ref = build_dataset_ref(path, clock=fake_clock([t0]))

        assert ref.created_at == t0.isoformat()


# ─────────────────────────────────────────────────────────────────
# build_dataset_ref — unsupported formats
# ─────────────────────────────────────────────────────────────────

class TestBuildDatasetRefUnsupportedFormat:

    def test_raises_value_error_for_unsupported_extension(self, tmp_path):
        path = tmp_path / "data.txt"
        path.write_text("not a supported format")

        with pytest.raises(ValueError, match="Unsupported dataset format"):
            build_dataset_ref(path)

    def test_error_message_names_the_offending_path(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text("{}")

        with pytest.raises(ValueError, match=r"data\.json"):
            build_dataset_ref(path)


# ─────────────────────────────────────────────────────────────────
# DatasetRef — plain dataclass behavior
# ─────────────────────────────────────────────────────────────────

class TestDatasetRefEquality:

    def test_two_refs_with_same_fields_are_equal(self):
        ref_a = DatasetRef(path=Path("data.csv"), row_count=5, schema_hash="abc", created_at="t")
        ref_b = DatasetRef(path=Path("data.csv"), row_count=5, schema_hash="abc", created_at="t")

        assert ref_a == ref_b

    def test_refs_with_different_row_counts_are_not_equal(self):
        ref_a = DatasetRef(path=Path("data.csv"), row_count=5, schema_hash="abc", created_at="t")
        ref_b = DatasetRef(path=Path("data.csv"), row_count=6, schema_hash="abc", created_at="t")

        assert ref_a != ref_b