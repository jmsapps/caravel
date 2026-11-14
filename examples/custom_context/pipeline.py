"""Wrapping the framework step context in an application context.

``context_factory`` receives Caravel's :class:`StepContext` and returns
whatever the application wants steps to see. It cannot change scheduling,
progress, or commits, so it stays an application concern.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from caravel import (
    DurableStorage,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    StepContext,
    run,
)

PIPELINE_NAME = "custom_context_example"
STAGE_NAME = "summary"
STEP_NAME = "summarize"

PARTITIONS: dict[str, dict[str, Any]] = {
    "alpha": {"id": "alpha", "value": 2},
    "beta": {"id": "beta", "value": 3},
}


@dataclass(frozen=True)
class JobContext:
    """An application context carrying the framework context plus extras."""

    framework: StepContext
    job_name: str

    @property
    def params(self) -> Any:
        return self.framework.params


def make_job_context(context: StepContext) -> JobContext:
    return JobContext(framework=context, job_name=context.pipeline_name.upper())


def summarize(inputs, *, context: JobContext) -> dict[str, Any]:
    """Aggregate every partition behind an explicit all-input barrier."""
    records = {partition.partition_id: partition.load() for partition in inputs.partitions()}
    return {
        "job_name": context.job_name,
        "count": len(records),
        "total": sum(int(record["value"]) for record in records.values()),
        "ids": sorted(records),
    }


def build_custom_context_pipeline() -> Pipeline:
    from caravel import CallableLoader

    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(
                name=STAGE_NAME,
                entries=[
                    Step(
                        summarize,
                        input_scope="all",
                        output=JSONDataset(name="summary"),
                    )
                ],
            )
        ],
        source=CallableLoader("partitions", lambda: PARTITIONS),
    )


def run_custom_context_pipeline(*, state_root: Path | str, job_id: str = "custom-context-1") -> Any:
    """Run the example with an application-supplied context factory."""
    return run(
        build_custom_context_pipeline(),
        job_id=job_id,
        storage=DurableStorage.at(Path(state_root) / "output"),
        context_factory=make_job_context,
    )


__all__ = [
    "PIPELINE_NAME",
    "STAGE_NAME",
    "STEP_NAME",
    "JobContext",
    "build_custom_context_pipeline",
    "make_job_context",
    "run_custom_context_pipeline",
]
