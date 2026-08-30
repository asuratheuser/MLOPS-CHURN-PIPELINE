"""Tests for src.ingestion.ingestion: run."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.types import StageStatus
from src.ingestion.ingestion import (
    IngestionStageFailed,
    run,
)
from src.ingestion.manifest import IngestionManifest


def make_config(items) -> dict:
    return {"items": items}


def read_manifest_entries(manifest_path: Path) -> list[dict]:
    if not manifest_path.exists():
        return []
    return [json.loads(line) for line in manifest_path.read_text().splitlines()]


def make_item(name: str, expected_hash: str = "MATCH") -> dict:
    return {
        "url": f"https://example.com/{name}",
        "filename": name,
        "expected_hash": expected_hash,
    }


# ─────────────────────────────────────────────────────────────────
# Config handling
# ─────────────────────────────────────────────────────────────────

class TestConfigHandling:

    def test_raises_value_error_when_items_key_missing(self, tmp_path):
        config = {"not_items": []}
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(ValueError, match="items"):
            run(config, manifest_path)

    def test_empty_items_list_succeeds_with_no_entries(self, tmp_path):
        config = make_config(items=[])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)

        assert result.total == 0
        assert result.succeeded == []
        assert result.failed == []

    def test_items_that_is_not_a_list_raises_type_error(self, tmp_path):
        # config["items"] is a dict instead of a list — iterating yields
        # its keys (strings), and item["url"] on a string then raises
        # TypeError rather than anything ingestion-specific. This isn't
        # caught or converted anywhere, so it should surface as-is.
        config = make_config(items={"not": "a list"})
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(TypeError):
            run(config, manifest_path)


# ─────────────────────────────────────────────────────────────────
# Manifest construction failure (before the loop even starts)
# ─────────────────────────────────────────────────────────────────

class TestManifestConstructionFailure:

    def test_manifest_parent_path_is_a_file_raises_not_a_directory_error(self, tmp_path):
        # If manifest_path's parent already exists as a plain file (not
        # a directory), IngestionManifest's own ensure_dir_exists call
        # raises NotADirectoryError. This happens before StageResult.start
        # or the item loop, so no IngestionResult is ever produced —
        # confirms this fails loudly rather than silently.
        blocking_file = tmp_path / "not_a_directory"
        blocking_file.write_text("i am a file, not a directory")
        manifest_path = blocking_file / "manifest.jsonl"

        config = make_config(items=[])

        with pytest.raises(NotADirectoryError):
            run(config, manifest_path)


# ─────────────────────────────────────────────────────────────────
# Single item — success / corrupt / failure
# ─────────────────────────────────────────────────────────────────

class TestSingleItemOutcomes:

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_success_writes_verified_entry_and_returns_no_failure(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        output_file = tmp_path / "data.zip"
        output_file.write_bytes(b"real content")
        mock_path.return_value = output_file
        mock_hash.return_value = "MATCH"

        config = make_config(items=[make_item("data.zip", "MATCH")])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)

        assert len(result.succeeded) == 1
        assert result.failed == []
        assert result.succeeded[0].status == "verified"
        mock_download.assert_called_once()

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "verified"
        assert entries[0]["sha256"] == "MATCH"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_hash_mismatch_writes_corrupt_entry_and_raises(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        output_file = tmp_path / "data.zip"
        output_file.write_bytes(b"wrong content")
        mock_path.return_value = output_file
        mock_hash.return_value = "ACTUAL_HASH"

        config = make_config(items=[make_item("data.zip", "EXPECTED_HASH")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run(config, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "corrupt"
        assert entries[0]["sha256"] == "ACTUAL_HASH"
        assert entries[0]["expected_hash"] == "EXPECTED_HASH"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_hash_match_with_different_case_is_treated_as_corrupt(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        # CHARACTERIZATION TEST — documents current behavior, not a
        # guarantee it's correct. actual_hash/expected_hash are compared
        # with a plain "==", so a hash that matches except for letter
        # case is treated as a mismatch (marked corrupt) even though the
        # underlying file content is identical. This is a real gap: hex
        # digests are conventionally case-insensitive, and a config
        # author writing an uppercase hash would see every otherwise-
        # good file rejected as "corrupt". Flagging here rather than
        # silently fixing it, since the fix belongs in ingestion.py, not
        # in this test file.
        output_file = tmp_path / "data.zip"
        output_file.write_bytes(b"real content")
        mock_path.return_value = output_file
        mock_hash.return_value = "abcdef0123"

        config = make_config(items=[make_item("data.zip", "ABCDEF0123")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed):
            run(config, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "corrupt"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.download_stream")
    def test_download_failure_writes_failed_entry_and_raises(
        self, mock_download, mock_path, tmp_path
    ):
        output_file = tmp_path / "data.zip"
        mock_path.return_value = output_file
        mock_download.side_effect = ConnectionError("network unreachable")

        config = make_config(items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run(config, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "failed"
        assert entries[0]["error"] == "network unreachable"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.download_stream")
    def test_missing_output_file_after_download_raises_file_not_found(
        self, mock_download, mock_path, tmp_path
    ):
        # download_stream is mocked to a no-op, so output_path is never
        # actually created. calculate_sha256 (unmocked here) then tries
        # to open a file that doesn't exist — this is a real path
        # calculate_sha256 can hit if a download silently produces no
        # file, and it's caught by _ingest_one_item's broad except,
        # same as any other per-item failure.
        output_file = tmp_path / "data.zip"  # never created
        mock_path.return_value = output_file
        mock_download.return_value = None

        config = make_config(items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run(config, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "failed"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    def test_path_traversal_in_filename_is_treated_as_item_failure(self, mock_path, tmp_path):
        # get_raw_data_path's own guard raises ValueError — confirm it's
        # caught the same as any other per-item failure, not left to crash
        # the whole batch
        mock_path.side_effect = ValueError("filename escapes the raw data directory")

        config = make_config(items=[make_item("../../etc/passwd")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run(config, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert len(entries) == 1
        assert entries[0]["status"] == "failed"
        assert "escapes the raw data directory" in entries[0]["error"]

    def test_malformed_item_missing_required_key_raises_uncaught_key_error(self, tmp_path):
        # BUG CHARACTERIZATION: url/filename/expected_hash lookups and
        # manifest.start_run() in _ingest_one_item happen OUTSIDE its own
        # try/except. A single malformed item (missing a required key)
        # therefore raises KeyError straight out of the loop in run(),
        # aborting the entire batch — no IngestionResult is ever
        # produced, and no other items in the batch get attempted. This
        # directly contradicts the module's own stated contract ("one
        # bad item does not abort the batch"). This test exists to make
        # that gap visible and failing loudly, not to endorse it — see
        # the BUG comment in _ingest_one_item.
        malformed_item = {"url": "https://example.com/data.zip", "filename": "data.zip"}
        # missing "expected_hash"

        config = make_config(items=[malformed_item])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(KeyError):
            run(config, manifest_path)

        # nothing was ever written to the manifest — the batch aborted
        # before any per-item outcome could be recorded
        assert read_manifest_entries(manifest_path) == []


# ─────────────────────────────────────────────────────────────────
# Batch behavior — multiple items, isolation
# ─────────────────────────────────────────────────────────────────

class TestBatchIsolation:

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_one_item_failing_does_not_stop_others_from_being_attempted(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        def fake_path(filename):
            path = tmp_path / filename
            path.write_bytes(b"content")
            return path

        mock_path.side_effect = fake_path
        mock_hash.return_value = "MATCH"

        def fake_download(url, output_path, timeout=15.0):
            if "bad" in url:
                raise ConnectionError("simulated failure")

        mock_download.side_effect = fake_download

        items = [make_item("good1.zip"), make_item("bad.zip"), make_item("good2.zip")]
        config = make_config(items=items)
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 3"):
            run(config, manifest_path)

        assert mock_download.call_count == 3  # all three were attempted
        entries = read_manifest_entries(manifest_path)
        assert len(entries) == 3
        statuses = {e["local_path"].split("/")[-1]: e["status"] for e in entries}
        assert statuses["good1.zip"] == "verified"
        assert statuses["bad.zip"] == "failed"
        assert statuses["good2.zip"] == "verified"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_all_items_succeeding_does_not_raise(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        def fake_path(filename):
            path = tmp_path / filename
            path.write_bytes(b"content")
            return path

        mock_path.side_effect = fake_path
        mock_hash.return_value = "MATCH"

        items = [make_item("a.zip"), make_item("b.zip")]
        config = make_config(items=items)
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)  # must not raise

        assert len(result.succeeded) == 2
        assert result.failed == []


# ─────────────────────────────────────────────────────────────────
# StageResult fields — stage_name, status, timestamps, duration
# ─────────────────────────────────────────────────────────────────

class TestStageResultFields:

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_successful_run_reports_success_status_and_stage_name(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        def fake_path(filename):
            path = tmp_path / filename
            path.write_bytes(b"content")
            return path

        mock_path.side_effect = fake_path
        mock_hash.return_value = "MATCH"

        config = make_config(items=[make_item("a.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)

        assert result.stage_name == "ingestion"
        assert result.status == StageStatus.SUCCESS

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_successful_run_has_sane_timestamps_and_duration(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        def fake_path(filename):
            path = tmp_path / filename
            path.write_bytes(b"content")
            return path

        mock_path.side_effect = fake_path
        mock_hash.return_value = "MATCH"

        config = make_config(items=[make_item("a.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)

        assert result.duration_seconds >= 0
        assert result.completed_at >= result.started_at

    def test_empty_batch_still_reports_success_status(self, tmp_path):
        config = make_config(items=[])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)

        assert result.stage_name == "ingestion"
        assert result.status == StageStatus.SUCCESS

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.download_stream")
    def test_failed_run_reports_failed_status(self, mock_download, mock_path, tmp_path):
        output_file = tmp_path / "data.zip"
        mock_path.return_value = output_file
        mock_download.side_effect = ConnectionError("network unreachable")

        config = make_config(items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed) as exc_info:
            run(config, manifest_path)

        assert exc_info.value.result.status == StageStatus.FAILED
        assert exc_info.value.result.stage_name == "ingestion"


# ─────────────────────────────────────────────────────────────────
# IngestionStageFailed.result — the exception carries the finalized result
# ─────────────────────────────────────────────────────────────────

class TestIngestionStageFailedResult:

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.calculate_sha256")
    @patch("src.ingestion.ingestion.download_stream")
    def test_result_reflects_mixed_success_and_failure(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        def fake_path(filename):
            path = tmp_path / filename
            path.write_bytes(b"content")
            return path

        mock_path.side_effect = fake_path
        mock_hash.return_value = "MATCH"

        def fake_download(url, output_path, timeout=15.0):
            if "bad" in url:
                raise ConnectionError("simulated failure")

        mock_download.side_effect = fake_download

        items = [make_item("good.zip"), make_item("bad.zip")]
        config = make_config(items=items)
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed) as exc_info:
            run(config, manifest_path)

        result = exc_info.value.result
        assert len(result.succeeded) == 1
        assert len(result.failed) == 1
        assert result.total == 2

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.download_stream")
    def test_result_is_a_real_ingestion_result_instance(
        self, mock_download, mock_path, tmp_path
    ):
        from src.ingestion.ingestion import IngestionResult

        output_file = tmp_path / "data.zip"
        mock_path.return_value = output_file
        mock_download.side_effect = ConnectionError("network unreachable")

        config = make_config(items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed) as exc_info:
            run(config, manifest_path)

        assert isinstance(exc_info.value.result, IngestionResult)


# ─────────────────────────────────────────────────────────────────
# The "manifest logging itself fails" fallback
# ─────────────────────────────────────────────────────────────────

class TestManifestLoggingFailureFallback:

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.download_stream")
    def test_does_not_crash_or_mask_original_error_when_manifest_write_also_fails(
        self, mock_download, mock_path, tmp_path, caplog
    ):
        output_file = tmp_path / "data.zip"
        mock_path.return_value = output_file
        mock_download.side_effect = ConnectionError("network unreachable")

        config = make_config(items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with (
            patch.object(IngestionManifest, "log_failure", side_effect=OSError("disk full")),
            caplog.at_level("CRITICAL"),
            pytest.raises(IngestionStageFailed),
        ):
            run(config, manifest_path)

        # the original ConnectionError must not leak out in place of
        # IngestionStageFailed, and the failure must still be visible
        # via the logging fallback
        assert "network unreachable" in caplog.text