"""Tests for src.ingestion.ingestion: run_ingestion_stage."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from src.ingestion.ingestion import (
    IngestionStageFailed,
    run_ingestion_stage,
)
from src.ingestion.manifest import IngestionManifest


def write_config(tmp_path: Path, items: list[dict]) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump({"items": items}))
    return config_path


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

    def test_raises_file_not_found_for_missing_config(self, tmp_path):
        missing_config = tmp_path / "does_not_exist.yaml"
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(FileNotFoundError):
            run_ingestion_stage(missing_config, manifest_path)

    def test_raises_value_error_when_items_key_missing(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump({"not_items": []}))
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(ValueError, match="items"):
            run_ingestion_stage(config_path, manifest_path)

    def test_empty_items_list_succeeds_with_no_entries(self, tmp_path):
        config_path = write_config(tmp_path, items=[])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run_ingestion_stage(config_path, manifest_path)

        assert result.total == 0
        assert result.succeeded == []
        assert result.failed == []


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

        config_path = write_config(tmp_path, items=[make_item("data.zip", "MATCH")])
        manifest_path = tmp_path / "manifest.jsonl"

        result = run_ingestion_stage(config_path, manifest_path)

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

        config_path = write_config(tmp_path, items=[make_item("data.zip", "EXPECTED_HASH")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run_ingestion_stage(config_path, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "corrupt"
        assert entries[0]["sha256"] == "ACTUAL_HASH"
        assert entries[0]["expected_hash"] == "EXPECTED_HASH"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    @patch("src.ingestion.ingestion.download_stream")
    def test_download_failure_writes_failed_entry_and_raises(
        self, mock_download, mock_path, tmp_path
    ):
        output_file = tmp_path / "data.zip"
        mock_path.return_value = output_file
        mock_download.side_effect = ConnectionError("network unreachable")

        config_path = write_config(tmp_path, items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run_ingestion_stage(config_path, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert entries[0]["status"] == "failed"
        assert entries[0]["error"] == "network unreachable"

    @patch("src.ingestion.ingestion.get_raw_data_path")
    def test_path_traversal_in_filename_is_treated_as_item_failure(self, mock_path, tmp_path):
        # get_raw_data_path's own guard raises ValueError — confirm it's
        # caught the same as any other per-item failure, not left to crash
        # the whole batch
        mock_path.side_effect = ValueError("filename escapes the raw data directory")

        config_path = write_config(tmp_path, items=[make_item("../../etc/passwd")])
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 1"):
            run_ingestion_stage(config_path, manifest_path)

        entries = read_manifest_entries(manifest_path)
        assert len(entries) == 1
        assert entries[0]["status"] == "failed"
        assert "escapes the raw data directory" in entries[0]["error"]


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
        config_path = write_config(tmp_path, items=items)
        manifest_path = tmp_path / "manifest.jsonl"

        with pytest.raises(IngestionStageFailed, match="1 of 3"):
            run_ingestion_stage(config_path, manifest_path)

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
        config_path = write_config(tmp_path, items=items)
        manifest_path = tmp_path / "manifest.jsonl"

        result = run_ingestion_stage(config_path, manifest_path)  # must not raise

        assert len(result.succeeded) == 2
        assert result.failed == []


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

        config_path = write_config(tmp_path, items=[make_item("data.zip")])
        manifest_path = tmp_path / "manifest.jsonl"

        with patch.object(
            IngestionManifest, "log_failure", side_effect=OSError("disk full")
        ):
            with caplog.at_level("CRITICAL"):
                with pytest.raises(IngestionStageFailed):
                    run_ingestion_stage(config_path, manifest_path)

        # the original ConnectionError must not leak out in place of
        # IngestionStageFailed, and the failure must still be visible
        # via the logging fallback
        assert "network unreachable" in caplog.text