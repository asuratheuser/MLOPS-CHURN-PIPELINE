"""Tests for src.core.types: StageResult, DatasetRef, and their helpers."""

import csv
from dataclasses import dataclass
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

    def test_clock_going_backward_clamps_duration_to_zero(self):
        # FIX VERIFICATION: finish() now clamps duration_seconds to 0.0
        # rather than returning a negative value when completed_at ends
        # up earlier than started_at (real clock skew, or a misused
        # injected clock). Previously this silently returned a negative
        # number — this test now asserts the clamp actually happens.
        t0 = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)  # earlier than t0
        clock = fake_clock([t0, t1])

        open_stage = StageResult.start("ingestion", clock=clock)
        result = open_stage.finish(FakeStageResult, StageStatus.SUCCESS)

        assert result.duration_seconds == 0.0

    def test_finish_field_colliding_with_base_field_name_raises_type_error(self):
        # Passing a field via **fields that collides with one of
        # StageResult's own base fields (e.g. stage_name) raises
        # TypeError, since finish() already supplies stage_name
        # explicitly — this documents that collision is a hard error,
        # not silently overwritten.
        open_stage = StageResult.start("ingestion")

        with pytest.raises(TypeError):
            open_stage.finish(FakeStageResult, StageStatus.SUCCESS, stage_name="collision")

    def test_finish_missing_required_subclass_field_raises_type_error(self):
        # FakeStageResult's items_processed has a default (=0), so this
        # specific subclass can't demonstrate a missing-required-field
        # error via its own fields. Using a subclass with a field that
        # has no default confirms finish() doesn't silently supply one.
        @dataclass
        class StrictFakeResult(StageResult):
            required_field: int  # no default

        open_stage = StageResult.start("ingestion")

        with pytest.raises(TypeError):
            open_stage.finish(StrictFakeResult, StageStatus.SUCCESS)  # required_field omitted


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

    def test_duplicate_column_names_are_hashed_as_is(self):
        # Not deduplicated — two columns named "a" produce a different
        # hash than a single "a", which is arguably correct (a schema
        # with a genuine duplicate column IS a different schema).
        assert compute_schema_hash(["a", "a", "b"]) != compute_schema_hash(["a", "b"])

    def test_mixed_type_column_names_raises_type_error(self):
        # EDGE CASE: sorted() on a mixed-type list (str and int here)
        # raises TypeError. The real reader always produces all-string
        # column names, so this is only reachable via direct/malformed
        # calls to compute_schema_hash — documented, not fixed, since
        # the function's contract (per its docstring) assumes a list of
        # strings.
        with pytest.raises(TypeError):
            compute_schema_hash(["a", 1, "b"])


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

    def test_uppercase_extension_is_treated_as_csv(self, tmp_path):
        path = tmp_path / "DATA.CSV"
        write_csv(path, columns=["id"], rows=[["1"], ["2"]])

        ref = build_dataset_ref(path)

        assert ref.row_count == 2

    def test_trailing_blank_line_inflates_row_count_by_one(self, tmp_path):
        # VULNERABILITY CHARACTERIZATION: a manually-written file with a
        # trailing blank line after the last real row. csv.reader yields
        # an empty [] for that blank line, and _read_header_and_count_csv
        # counts it like any other row. This documents the known quirk
        # noted in the function's docstring rather than silently hiding
        # it.
        path = tmp_path / "data.csv"
        path.write_text("id,value\n1,a\n2,b\n\n")  # note trailing blank line

        ref = build_dataset_ref(path)

        assert ref.row_count == 3  # 2 real rows + 1 blank line miscounted

    def test_missing_file_raises_file_not_found_error(self, tmp_path):
        path = tmp_path / "does_not_exist.csv"

        with pytest.raises(FileNotFoundError):
            build_dataset_ref(path)

    def test_directory_path_raises_platform_specific_error(self, tmp_path):
        # Opening a directory as if it were a file raises IsADirectoryError
        # on Linux/Mac, but PermissionError on Windows — this is an OS-level
        # inconsistency, not something build_dataset_ref controls, so the
        # test accepts either.
        directory = tmp_path / "a_directory.csv"
        directory.mkdir()

        with pytest.raises((IsADirectoryError, PermissionError)):
            build_dataset_ref(directory)


# ─────────────────────────────────────────────────────────────────
# build_dataset_ref — unsupported formats
# ─────────────────────────────────────────────────────────────────

class TestBuildDatasetRefUnsupportedFormat:

    def test_raises_value_error_for_unsupported_extension(self, tmp_path):
        path = tmp_path / "data.txt"
        path.write_text("not a supported format")

        with pytest.raises(ValueError, match="Unsupported dataset format"):
            build_dataset_ref(path)

    def test_parquet_is_unsupported(self, tmp_path):
        # .parquet support was deliberately removed (YAGNI — no stage in
        # this pipeline currently produces or consumes Parquet). This
        # test locks in that .parquet now falls through to the generic
        # unsupported-format error, same as any other unrecognized
        # extension, rather than being silently treated as valid.
        path = tmp_path / "data.parquet"
        path.write_bytes(b"not real parquet content")

        with pytest.raises(ValueError, match="Unsupported dataset format"):
            build_dataset_ref(path)

    def test_error_message_names_the_offending_path(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text("{}")

        with pytest.raises(ValueError, match=r"data\.json"):
            build_dataset_ref(path)

    def test_no_extension_at_all_raises_value_error(self, tmp_path):
        path = tmp_path / "data_no_extension"
        path.write_text("some content")

        with pytest.raises(ValueError, match="Unsupported dataset format"):
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