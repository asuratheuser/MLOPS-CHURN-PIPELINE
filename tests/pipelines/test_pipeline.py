"""Tests for the pipeline orchestrator skeleton."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.pipelines.pipeline import run_pipeline


def test_run_pipeline_explicitly_marks_unimplemented_workflow() -> None:
    """The skeleton must fail clearly, never pretend an empty run succeeded."""
    with pytest.raises(NotImplementedError, match="stage execution has not been implemented"):
        run_pipeline(SimpleNamespace(), SimpleNamespace())
