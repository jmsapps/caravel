from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from caravel import CallableLoader, JSONDataset, Pipeline, Stage, StepContext, run, step


@dataclass(frozen=True)
class JobContext:
    base: StepContext
    tenant: str
    job_id: str


def make_job_context(base: StepContext) -> JobContext:
    return JobContext(base=base, tenant="acme", job_id="job-001")


def _load_partitions() -> dict[str, dict[str, Any]]:
    return {
        "a": {"id": "a", "value": 3},
        "b": {"id": "b", "value": 7},
    }


@step(output=JSONDataset())
def enrich_with_job_context(
    partitions: dict[str, dict[str, Any]], *, context: JobContext
) -> dict[str, dict[str, Any]]:
    return {
        key: {
            **record,
            "tenant": context.tenant,
            "job_id": context.job_id,
            "run_id": context.base.run_id,
        }
        for key, record in partitions.items()
    }


@step(output=JSONDataset())
def summarize(partitions: dict[str, dict[str, Any]], *, context: JobContext) -> dict[str, Any]:
    return {
        "tenant": context.tenant,
        "job_id": context.job_id,
        "step": context.base.step_name,
        "count": len(partitions),
        "total": sum(int(record["value"]) for record in partitions.values()),
    }


def build_custom_context_pipeline() -> Pipeline:
    return Pipeline(
        name="custom_context_example",
        loader=CallableLoader("inline_records", _load_partitions),
        stages=[
            Stage(name="bronze", entries=[enrich_with_job_context]),
            Stage(name="silver", entries=[summarize]),
        ],
    )


def run_custom_context_pipeline(run_root: Path | str | None = None) -> Path | str:
    pipeline = build_custom_context_pipeline()
    return run(pipeline, run_root=run_root, context_factory=make_job_context)


__all__ = [
    "JobContext",
    "build_custom_context_pipeline",
    "make_job_context",
    "run_custom_context_pipeline",
]
