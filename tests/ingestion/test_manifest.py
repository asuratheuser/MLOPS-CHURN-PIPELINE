"""Tests for src.ingestion.manifest: IngestionManifest."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.ingestion.manifest import IngestionManifest, UnknownRunError


def fixed_clock(fixed_time: datetime):
    """Returns a zero-arg callable usable as IngestionManifest's clock
    injection point, always returning the same fixed instant."""
    return lambda: fixed_time


def read_entries(path: Path) -> list[dict]:
    """Reads a JSONL manifest file back into a list of dicts."""
    return [json.loads(line) for line in path.read_text().splitlines()]


# ─────────────────────────────────────────────────────────────────
# Construction
# ─────────────────────────────────────────────────────────────────

class TestConstruction:

    def test_creates_parent_directory_if_missing(self, tmp_path):
        output_path = tmp_path / "nested" / "manifest.jsonl"

        IngestionManifest(output_path)

        assert output_path.parent.is_dir()

    def test_does_not_create_the_manifest_file_itself_until_first_write(self, tmp_path):
        output_path = tmp_path / "manifest.jsonl"

        IngestionManifest(output_path)

        assert not output_path.exists()

    def test_raises_if_parent_path_is_a_file(self, tmp_path):
        blocker = tmp_path / "blocker"
        blocker.touch()
        output_path = blocker / "manifest.jsonl"

        with pytest.raises(NotADirectoryError):
            IngestionManifest(output_path)


# ─────────────────────────────────────────────────────────────────
# start_run
# ─────────────────────────────────────────────────────────────────

class TestStartRun:

    def test_returns_a_run_id_string(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")

        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))

        assert isinstance(run_id, str)
        assert len(run_id) > 0

    def test_successive_calls_return_different_run_ids(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")

        first = manifest.start_run("https://example.com/a.zip", Path("/data/a.zip"))
        second = manifest.start_run("https://example.com/b.zip", Path("/data/b.zip"))

        assert first != second

    def test_records_started_at_using_injected_clock(self, tmp_path):
        fixed_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        manifest = IngestionManifest(tmp_path / "manifest.jsonl", clock=fixed_clock(fixed_time))

        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))
        manifest.log_success(run_id, sha256="abc", file_size_bytes=1)

        entry = read_entries(tmp_path / "manifest.jsonl")[0]
        assert entry["started_at"] == fixed_time.isoformat()

    def test_does_not_write_anything_to_disk_by_itself(self, tmp_path):
        output_path = tmp_path / "manifest.jsonl"
        manifest = IngestionManifest(output_path)

        manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))

        assert not output_path.exists()  # nothing written until a log_* call


# ─────────────────────────────────────────────────────────────────
# log_success
# ─────────────────────────────────────────────────────────────────

class TestLogSuccess:

    def test_writes_one_jsonl_line_with_expected_fields(self, tmp_path):
        output_path = tmp_path / "manifest.jsonl"
        fixed_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        manifest = IngestionManifest(output_path, clock=fixed_clock(fixed_time))

        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))
        manifest.log_success(run_id, sha256="abc123", file_size_bytes=2048)

        entries = read_entries(output_path)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["run_id"] == run_id
        assert entry["source_url"] == "https://example.com/f.zip"
        assert entry["local_path"] == str(Path("/data/f.zip"))
        assert entry["status"] == "verified"
        assert entry["sha256"] == "abc123"
        assert entry["file_size_bytes"] == 2048
        assert entry["started_at"] == fixed_time.isoformat()
        assert entry["completed_at"] == fixed_time.isoformat()
        assert "error" not in entry
        assert "expected_hash" not in entry

    def test_removes_run_from_active_runs(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")
        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))

        manifest.log_success(run_id, sha256="abc", file_size_bytes=1)

        assert run_id not in manifest._active_runs

    def test_raises_unknown_run_error_for_unstarted_run_id(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")

        with pytest.raises(UnknownRunError):
            manifest.log_success("never-started", sha256="abc", file_size_bytes=1)

    def test_raises_unknown_run_error_if_already_logged(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")
        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))
        manifest.log_success(run_id, sha256="abc", file_size_bytes=1)

        with pytest.raises(UnknownRunError):
            manifest.log_success(run_id, sha256="abc", file_size_bytes=1)


# ─────────────────────────────────────────────────────────────────
# log_corrupt
# ─────────────────────────────────────────────────────────────────

class TestLogCorrupt:

    def test_writes_entry_with_status_corrupt_and_expected_hash(self, tmp_path):
        output_path = tmp_path / "manifest.jsonl"
        manifest = IngestionManifest(output_path)
        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))

        manifest.log_corrupt(run_id, sha256="bad_hash", file_size_bytes=512, expected_hash="good_hash")

        entry = read_entries(output_path)[0]
        assert entry["status"] == "corrupt"
        assert entry["sha256"] == "bad_hash"
        assert entry["expected_hash"] == "good_hash"
        assert entry["file_size_bytes"] == 512
        assert "error" not in entry

    def test_raises_unknown_run_error_for_unstarted_run_id(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")

        with pytest.raises(UnknownRunError):
            manifest.log_corrupt("never-started", sha256="x", file_size_bytes=1, expected_hash="y")


# ─────────────────────────────────────────────────────────────────
# log_failure
# ─────────────────────────────────────────────────────────────────

class TestLogFailure:

    def test_writes_entry_with_status_failed_and_error(self, tmp_path):
        output_path = tmp_path / "manifest.jsonl"
        manifest = IngestionManifest(output_path)
        run_id = manifest.start_run("https://example.com/f.zip", Path("/data/f.zip"))

        manifest.log_failure(run_id, error="connection refused")

        entry = read_entries(output_path)[0]
        assert entry["status"] == "failed"
        assert entry["error"] == "connection refused"
        assert "sha256" not in entry
        assert "file_size_bytes" not in entry
        assert "expected_hash" not in entry

    def test_raises_unknown_run_error_for_unstarted_run_id(self, tmp_path):
        manifest = IngestionManifest(tmp_path / "manifest.jsonl")

        with pytest.raises(UnknownRunError):
            manifest.log_failure("never-started", error="boom")


# ─────────────────────────────────────────────────────────────────
# Multiple runs / file behavior
# ─────────────────────────────────────────────────────────────────

class TestMultipleRuns:

    def test_concurrent_in_flight_runs_do_not_interfere(self, tmp_path):
        # two runs started before either finishes — active_runs must
        # keep them independent, keyed correctly by run_id
        output_path = tmp_path / "manifest.jsonl"
        manifest = IngestionManifest(output_path)

        run_a = manifest.start_run("https://example.com/a.zip", Path("/data/a.zip"))
        run_b = manifest.start_run("https://example.com/b.zip", Path("/data/b.zip"))

        manifest.log_failure(run_b, error="b failed")
        manifest.log_success(run_a, sha256="a_hash", file_size_bytes=10)

        entries = {e["run_id"]: e for e in read_entries(output_path)}
        assert entries[run_a]["source_url"] == "https://example.com/a.zip"
        assert entries[run_a]["status"] == "verified"
        assert entries[run_b]["source_url"] == "https://example.com/b.zip"
        assert entries[run_b]["status"] == "failed"

    def test_each_run_appends_a_new_line_not_overwriting_previous(self, tmp_path):
        output_path = tmp_path / "manifest.jsonl"
        manifest = IngestionManifest(output_path)

        for i in range(3):
            run_id = manifest.start_run(f"https://example.com/{i}.zip", Path(f"/data/{i}.zip"))
            manifest.log_success(run_id, sha256=f"hash{i}", file_size_bytes=i)

        entries = read_entries(output_path)
        assert len(entries) == 3
        assert [e["file_size_bytes"] for e in entries] == [0, 1, 2]

    def test_manifest_file_persists_across_separate_instances(self, tmp_path):
        # a second IngestionManifest pointed at the same file should
        # append to existing history, not clobber it — real behavior
        # for a pipeline that runs repeatedly over time
        output_path = tmp_path / "manifest.jsonl"

        first_manifest = IngestionManifest(output_path)
        run_id = first_manifest.start_run("https://example.com/a.zip", Path("/data/a.zip"))
        first_manifest.log_success(run_id, sha256="a", file_size_bytes=1)

        second_manifest = IngestionManifest(output_path)
        run_id_2 = second_manifest.start_run("https://example.com/b.zip", Path("/data/b.zip"))
        second_manifest.log_success(run_id_2, sha256="b", file_size_bytes=2)

        entries = read_entries(output_path)
        assert len(entries) == 2
