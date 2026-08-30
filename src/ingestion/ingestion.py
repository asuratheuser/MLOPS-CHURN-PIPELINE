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

from src.core.types import StageResult, StageStatus
from src.ingestion.manifest import IngestionManifest
from src.storage.storage import get_raw_data_path
from src.utils.ingest import calculate_sha256, download_stream

logger = logging.getLogger(__name__)


class IngestionStageFailed(Exception):
    """Raised after a full ingestion run if one or more items failed
    or came back corrupt. Individual failures are already recorded in
    the manifest — this exception is only the batch-level signal that
    something in the run needs attention.

    Carries the finalized IngestionResult (result), so a caller that
    catches this exception can still inspect stage_name,
    duration_seconds, and exactly which items succeeded vs. failed —
    the same information that would have been available had the run
    succeeded outright.
    """

    def __init__(self, message: str, result: "IngestionResult"):
        super().__init__(message)
        self.result = result


@dataclass
class IngestionItemResult:
    """The outcome of ingesting a single configured item."""

    filename: str
    url: str
    run_id: str
    status: str  # "verified" | "corrupt" | "failed"
    error: str | None = None


@dataclass
class IngestionResult(StageResult):
    """The outcome of one full ingestion stage run."""

    succeeded: list[IngestionItemResult] = field(default_factory=list)
    failed: list[IngestionItemResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed)


def run(config: dict, manifest_path: Path) -> IngestionResult:
    """Runs the full ingestion stage: verifies every
    item listed in config, recording each outcome in the
    manifest at manifest_path.

    Args:
        config: config dictionary listing items to ingest.
            Each item is expected to provide url, filename, and
            expected_hash.
        manifest_path: Path to the manifest JSONL file to log into.

    Returns:
        An IngestionResult summarizing which items succeeded and
        which failed or came back corrupt.

    Raises:
        ValueError: if config has no "items" key. Raised before any
            timing/stage-result machinery starts, since this is a
            precondition failure, not a failed stage attempt — no
            ingestion work was ever tried.
        IngestionStageFailed: if one or more items failed or were
            corrupt. Raised only after every item has been attempted,
            and carries the finalized IngestionResult via .result.
    """
    if "items" not in config:
        raise ValueError(f"Config at {config} has no 'items' key")

    # Timing starts only now — after we know there's a legitimate
    # batch to actually attempt, not before input validation.
    open_stage = StageResult.start("ingestion")

    manifest = IngestionManifest(manifest_path)

    succeeded: list[IngestionItemResult] = []
    failed: list[IngestionItemResult] = []

    for item in config["items"]:
        item_result = _ingest_one_item(item, manifest)
        if item_result.status == "verified":
            succeeded.append(item_result)
        else:
            failed.append(item_result)

    result = open_stage.finish(
        IngestionResult,
        StageStatus.FAILED if failed else StageStatus.SUCCESS,
        succeeded=succeeded,
        failed=failed,
    )

    if result.failed:
        raise IngestionStageFailed(
            f"{len(result.failed)} of {result.total} item(s) failed or were corrupt",
            result=result,
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