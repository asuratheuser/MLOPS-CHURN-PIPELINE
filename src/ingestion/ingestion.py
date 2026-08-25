"""Ingestion stage orchestrator.

Sequences the per-item ingestion flow: resolve a destination path,
download, hash, verify against an expected hash, and record every
outcome in the manifest. One bad item does not abort the batch — all
items are attempted, and failures are only surfaced as a single
summary exception once the whole batch has been processed.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

from src.ingestion.manifest import IngestionManifest
from src.storage.storage import get_raw_data_path
from src.utils.ingest import calculate_sha256, download_stream, load_yaml_config

logger = logging.getLogger(__name__)


class IngestionStageFailed(Exception):
    """Raised after a full ingestion run if one or more items failed
    or came back corrupt. Individual failures are already recorded in
    the manifest — this exception is only the batch-level signal that
    something in the run needs attention."""


@dataclass
class IngestionItemResult:
    """The outcome of ingesting a single configured item."""

    filename: str
    url: str
    run_id: str
    status: str  # "verified" | "corrupt" | "failed"
    error: str | None = None


@dataclass
class IngestionResult:
    """The outcome of one full ingestion stage run."""

    succeeded: list[IngestionItemResult] = field(default_factory=list)
    failed: list[IngestionItemResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed)


def run_ingestion_stage(config_path: Path, manifest_path: Path) -> IngestionResult:
    """Runs the full ingestion stage: downloads and verifies every
    item listed in config_path, recording each outcome in the
    manifest at manifest_path.

    Args:
        config_path: Path to the YAML config listing items to ingest.
            Each item is expected to provide url, filename, and
            expected_hash.
        manifest_path: Path to the manifest JSONL file to log into.

    Returns:
        An IngestionResult summarizing which items succeeded and
        which failed or came back corrupt.

    Raises:
        IngestionStageFailed: if one or more items failed or were
            corrupt. Raised only after every item has been attempted.
    """
    config = load_yaml_config(config_path)
    if "items" not in config:
        raise ValueError(f"Config at {config_path} has no 'items' key")

    manifest = IngestionManifest(manifest_path)

    result = IngestionResult()

    for item in config["items"]:
        item_result = _ingest_one_item(item, manifest)
        if item_result.status == "verified":
            result.succeeded.append(item_result)
        else:
            result.failed.append(item_result)

    if result.failed:
        raise IngestionStageFailed(
            f"{len(result.failed)} of {result.total} item(s) failed or were corrupt"
        )

    return result


def _ingest_one_item(item: dict, manifest: IngestionManifest) -> IngestionItemResult:
    """Downloads, hashes, and verifies a single configured item,
    recording the outcome in the manifest. Never raises — any
    failure is caught, logged to the manifest, and reflected in the
    returned IngestionItemResult instead, so one bad item cannot
    abort the rest of the batch.
    """
    url = item["url"]
    filename = item["filename"]
    expected_hash = item["expected_hash"]
    
    # BUG: this line (and the dict lookups above it) run outside the
    # try/except below, so a bad config item or a future start_run()
    # failure would abort the whole batch instead of just this item.
    run_id = manifest.start_run(url, Path(filename))

    try:
        output_path = get_raw_data_path(filename)
        download_stream(url, output_path)
        actual_hash = calculate_sha256(output_path)
        file_size_bytes = output_path.stat().st_size

        if actual_hash == expected_hash:
            manifest.log_success(run_id, actual_hash, file_size_bytes)
            return IngestionItemResult(
                filename=filename, url=url, run_id=run_id, status="verified"
            )

        manifest.log_corrupt(run_id, actual_hash, file_size_bytes, expected_hash)
        return IngestionItemResult(
            filename=filename, url=url, run_id=run_id, status="corrupt"
        )

    except Exception as e:
        error_message = str(e)
        try:
            manifest.log_failure(run_id, error_message)
        except Exception:
            # Logging the failure itself failed (e.g. disk full). Don't
            # let that mask the original error or crash the batch —
            # fall back to plain logging so it's still visible somewhere.
            logger.critical(
                "Failed to record manifest entry for run_id=%s (original error: %s)",
                run_id,
                error_message,
                exc_info=True,
            )
        return IngestionItemResult(
            filename=filename,
            url=url,
            run_id=run_id,
            status="failed",
            error=error_message,
        )