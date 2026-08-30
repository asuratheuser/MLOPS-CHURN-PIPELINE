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

    def test_items_that_is_not_a_list_is_now_absorbed_as_a_per_item_failure(self, tmp_path):
        # BEHAVIOR CHANGE from the _ingest_one_item try-block fix: config
        # ["items"] being a dict (not a list) means iterating over it
        # yields its keys as plain strings. item["url"] on a string now
        # raises TypeError INSIDE _ingest_one_item's try block, so it's
        # caught like any other per-item failure instead of crashing the
        # whole run. This is a real design fork worth revisiting — should
        # a malformed items *structure* fail like a missing "items" key
        # (a precondition error, before any stage work starts), or like
        # this (a per-item failure)? Currently it's the latter; this test
        # locks in that choice until it's deliberately revisited.
        config = make_config(items={"not": "a list"})
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
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
    def test_hash_match_with_different_case_is_treated_as_verified(
        self, mock_download, mock_hash, mock_path, tmp_path
    ):
        # FIX VERIFICATION: actual_hash/expected_hash are now compared
        # case-insensitively. A hash that matches except for letter case
        # should be treated as a genuine match, not corruption.
        output_file = tmp_path / "data.zip"
        output_file.write_bytes(b"real content")
        mock_path.return_value = output_file
        mock_hash.return_value = "abcdef0123"

        config = make_config(items=[make_item("data.zip", "ABCDEF0123")])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run(config, manifest_path)  # must not raise

        assert result.succeeded[0].status == "verified"
        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "verified"

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
        # to open a file that doesn't exist — caught by _ingest_one_item's
        # broad except, same as any other per-item failure.
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

    def test_malformed_item_missing_required_key_is_treated_as_item_failure(self, tmp_path):
        # FIX VERIFICATION: url/filename/expected_hash lookups and
        # manifest.start_run() now happen INSIDE _ingest_one_item's try
        # block. A malformed item (missing a required key) is now caught
        # like any other per-item failure — it no longer aborts the whole
        # batch. run_id falls back to "unset" since no manifest run was
        # ever started for this item.
        malformed_item = {"url": "https://example.com/data.zip", "filename": "data.zip"}
        # missing "expected_hash"

        config = make_config(items=[malformed_item])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run(config, manifest_path)

        # nothing was written to the manifest for this item, since no
        # run was ever started — but the batch itself completed and
        # produced a normal IngestionStageFailed, not an uncaught KeyError
        assert read_manifest_entries(manifest_path) == []

    def test_malformed_item_does_not_abort_other_items_in_the_batch(self, tmp_path):
        # The real point of the fix: one malformed item must not stop
        # other, well-formed items in the same batch from being
        # attempted.
        malformed_item = {"url": "https://example.com/data.zip", "filename": "data.zip"}

        with (
            patch("src.ingestion.ingestion.get_raw_data_path") as mock_path,
            patch("src.ingestion.ingestion.calculate_sha256") as mock_hash,
            patch("src.ingestion.ingestion.download_stream"),
        ):
            def fake_path(filename):
                path = tmp_path / filename
                path.write_bytes(b"content")
                return path

            mock_path.side_effect = fake_path
            mock_hash.return_value = "MATCH"

            config = make_config(items=[malformed_item, make_item("good.zip")])
            manifest_path = tmp_path / "manifest.jsonl"

            with pytest.raises(IngestionStageFailed, match="1 of 2"):
                run(config, manifest_path)

        entries = read_manifest_entries(manifest_path)
        # only the well-formed item ever got far enough to start a run
        # and be logged — the malformed one has no manifest entry
        assert len(entries) == 1
        assert entries[0]["status"] == "verified"


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