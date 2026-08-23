"""Manifest audit logger for the ingestion stage.

Records the outcome of every ingestion run as a structured, append-only
JSONL entry. This class makes no judgment calls about success/failure/
corruption — it only records what the caller (the ingestion orchestrator)
tells it already happened.
"""

import json
import logging
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from src.storage.storage import ensure_dir_exists

logger = logging.getLogger(__name__)


class UnknownRunError(Exception):
    """Raised when a run_id passed to a log_* method was never started
    (never passed to start_run), or has already been logged once."""


class IngestionManifest:
    """Append-only audit log of ingestion runs.

    One instance is constructed with a target output path and reused
    across every run in a pipeline execution. Usage:

        manifest = IngestionManifest(output_path)
        run_id = manifest.start_run(source_url, local_path)
        try:
            ...
            manifest.log_success(run_id, sha256, file_size_bytes)
        except HashMismatch:
            manifest.log_corrupt(run_id, sha256, file_size_bytes, expected_hash)
        except Exception as e:
            manifest.log_failure(run_id, str(e))
    """

    def __init__(
        self,
        output_path: Path,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """
        Args:
            output_path: Full path to the manifest JSONL file. The
                caller is responsible for resolving where this should
                live (e.g. via config or get_project_root()-based
                logic) — this class only knows how to write to it.
                The parent directory is created automatically if it
                doesn't exist yet.
            clock: Optional zero-argument callable returning the
                current UTC time as a timezone-aware datetime.
                Defaults to the real system clock. Exposed so tests
                can inject a fixed or fake clock instead of asserting
                against real wall-clock time.
        """
        self.output_path = output_path
        ensure_dir_exists(self.output_path.parent)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._active_runs: dict[str, dict] = {}

    def start_run(self, source_url: str, local_path: Path) -> str:
        """Begins tracking a new ingestion run.

        Args:
            source_url: The URL being downloaded from.
            local_path: The destination path on disk.

        Returns:
            A newly generated run_id, to be passed to whichever
            log_* method matches the run's eventual outcome.
        """
        run_id = str(uuid.uuid4())
        self._active_runs[run_id] = {
            "source_url": source_url,
            "local_path": str(local_path),
            "started_at": self._now(),
        }
        return run_id

    def log_success(self, run_id: str, sha256: str, file_size_bytes: int) -> None:
        """Records a run that completed and matched its expected hash."""
        run = self._pop_active_run(run_id)
        entry = {
            **run,
            "run_id": run_id,
            "status": "verified",
            "completed_at": self._now(),
            "sha256": sha256,
            "file_size_bytes": file_size_bytes,
        }
        self._write_entry(entry)

    def log_corrupt(
        self,
        run_id: str,
        sha256: str,
        file_size_bytes: int,
        expected_hash: str,
    ) -> None:
        """Records a run that completed but did not match the expected hash."""
        run = self._pop_active_run(run_id)
        entry = {
            **run,
            "run_id": run_id,
            "status": "corrupt",
            "completed_at": self._now(),
            "sha256": sha256,
            "file_size_bytes": file_size_bytes,
            "expected_hash": expected_hash,
        }
        self._write_entry(entry)

    def log_failure(self, run_id: str, error: str) -> None:
        """Records a run that did not complete (network, HTTP, or filesystem error)."""
        run = self._pop_active_run(run_id)
        entry = {
            **run,
            "run_id": run_id,
            "status": "failed",
            "completed_at": self._now(),
            "error": error,
        }
        self._write_entry(entry)

    # ── internal helpers ────────────────────────────────────────────

    def _pop_active_run(self, run_id: str) -> dict:
        """Retrieves and removes the stored start_run() info for run_id
        — the run is now terminal, so there's no reason to keep it.

        Raises:
            UnknownRunError: if run_id was never started, or has
                already been logged (finished) once.
        """
        try:
            return self._active_runs.pop(run_id)
        except KeyError:
            raise UnknownRunError(
                f"No active run found for run_id={run_id!r}. It may never "
                "have been started, or may have already been logged."
            ) from None

    def _write_entry(self, entry: dict) -> None:
        """Appends one JSON entry as a single line to the manifest file.

        Raises:
            OSError: if the write itself fails (disk full, permissions,
                file removed out from under the process, etc.). The
                failure is logged before re-raising so the entry's
                content isn't lost to the void even though it never
                made it into the manifest file.
        """
        try:
            with open(self.output_path, mode="a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError:
            logger.error("Failed to write manifest entry: %s", entry)
            raise

    def _now(self) -> str:
        """Current time from this manifest's clock, as an ISO 8601 string."""
        return self._clock().isoformat()