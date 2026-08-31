"""Pipeline orchestration.

Stage implementations will be added incrementally. Keeping their ordering
here gives ``main.py`` one stable delegation point and makes the intended v1
workflow explicit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from main import PipelineConfig, RunContext


def run_pipeline(config: PipelineConfig, context: RunContext) -> object:
    """Run the v1 churn training workflow.

    Future implementation order: ingestion -> schema validation -> data
    quality -> processing -> feature engineering -> splitting -> training ->
    evaluation.
    """
    del config, context
    raise NotImplementedError(
        "The pipeline orchestrator is configured, but stage execution has not "
        "been implemented yet."
    )

