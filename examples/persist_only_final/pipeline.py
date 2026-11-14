"""Every step commits; aggregation is an explicit barrier.

The legacy ``persist=False`` option is gone: durable recovery requires each
step's output to be committed so a later invocation restores it instead of
recomputing. What used to be "only the last step persists" is now expressed as
cheap per-partition steps feeding one ```` aggregation.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from caravel import (
    RunResult,
    StepContext,
    CallableLoader,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)

from ..support import durable

PIPELINE_NAME = "persist_only_final_example"
STAGE_NAME = "pipeline"
STEP_1_NAME = "step_1_collect"
STEP_2_NAME = "step_2_scale"
STEP_3_NAME = "step_3_label"
STEP_4_NAME = "step_4_aggregate"
STEP_5_NAME = STEP_4_NAME
FINAL_STEP_NAME = STEP_4_NAME


class SeedRecord(TypedDict):
    """One record this example starts from."""

    id: str
    value: int


class CollectedRecord(SeedRecord):
    """A seed record after the collect step."""

    collected: bool


class ScaledRecord(CollectedRecord):
    """A collected record after the scale step."""

    scaled: int


class LabelledRecord(ScaledRecord):
    """A scaled record after the label step."""

    label: str


class Summary(TypedDict):
    """The aggregate view of every labelled record."""

    pipeline: str
    count: int
    ids: list[str]
    total_scaled: int


SeedRecords = dict[str, SeedRecord]
CollectedRecords = dict[str, CollectedRecord]
ScaledRecords = dict[str, ScaledRecord]
LabelledRecords = dict[str, LabelledRecord]

PARTITIONS: SeedRecords = {
    f"record-{index}": SeedRecord(id=f"record-{index}", value=index) for index in range(1, 5)
}


def step_1_collect(value: SeedRecords, *, context: StepContext) -> CollectedRecords:
    """Mark every incoming record as collected."""
    return {key: CollectedRecord(**record, collected=True) for key, record in value.items()}


def step_2_scale(value: CollectedRecords, *, context: StepContext) -> ScaledRecords:
    """Scale every collected record."""
    return {
        key: ScaledRecord(**record, scaled=int(record["value"]) * 10)
        for key, record in value.items()
    }


def step_3_label(value: ScaledRecords, *, context: StepContext) -> LabelledRecords:
    """Label every scaled record."""
    return {
        key: LabelledRecord(**record, label=f"v{record['scaled']}") for key, record in value.items()
    }


def step_4_aggregate(value: LabelledRecords, *, context: StepContext) -> Summary:
    """Aggregate every labelled record into one summary."""
    del context
    return Summary(
        pipeline=PIPELINE_NAME,
        count=len(value),
        ids=sorted(value),
        total_scaled=sum(int(record["scaled"]) for record in value.values()),
    )


def build_persist_only_final_pipeline() -> Pipeline:
    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(
                name=STAGE_NAME,
                entries=[
                    Step(step_1_collect, output=JSONDataset()),
                    Step(step_2_scale, output=JSONDataset()),
                    Step(step_3_label, output=JSONDataset()),
                    Step(
                        step_4_aggregate,
                        output=JSONDataset(name="aggregate"),
                    ),
                ],
            )
        ],
        source=CallableLoader("partitions", lambda: PARTITIONS),
    )


def run_persist_only_final_pipeline(
    *, state_root: Path | str, job_id: str = "persist-only-final-1"
) -> RunResult:
    return run(
        durable(build_persist_only_final_pipeline(), job_id), output=Path(state_root) / "output"
    )


__all__ = [
    "PIPELINE_NAME",
    "STAGE_NAME",
    "FINAL_STEP_NAME",
    "STEP_1_NAME",
    "STEP_2_NAME",
    "STEP_3_NAME",
    "STEP_4_NAME",
    "STEP_5_NAME",
    "PARTITIONS",
    "build_persist_only_final_pipeline",
    "run_persist_only_final_pipeline",
]
