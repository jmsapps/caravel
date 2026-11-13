"""Every step commits; aggregation is an explicit barrier.

The legacy ``persist=False`` option is gone: durable recovery requires each
step's output to be committed so a later invocation restores it instead of
recomputing. What used to be "only the last step persists" is now expressed as
cheap per-partition steps feeding one ``input_scope="all"`` aggregation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
    CallableLoader,
    DurableStorage,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
    step,
)

PIPELINE_NAME = "persist_only_final_example"
STAGE_NAME = "pipeline"
STEP_1_NAME = "step_1_collect"
STEP_2_NAME = "step_2_scale"
STEP_3_NAME = "step_3_label"
STEP_4_NAME = "step_4_aggregate"
STEP_5_NAME = STEP_4_NAME
FINAL_STEP_NAME = STEP_4_NAME

PARTITIONS: dict[str, dict[str, Any]] = {
    f"record-{index}": {"id": f"record-{index}", "value": index} for index in range(1, 5)
}


@step(output=JSONDataset())
def step_1_collect(record: dict[str, Any], *, context: object) -> dict[str, Any]:
    _ = context
    return {**record, "collected": True}


@step(output=JSONDataset())
def step_2_scale(record: dict[str, Any], *, context: object) -> dict[str, Any]:
    _ = context
    return {**record, "scaled": int(record["value"]) * 10}


@step(output=JSONDataset())
def step_3_label(record: dict[str, Any], *, context: object) -> dict[str, Any]:
    _ = context
    return {**record, "label": f"v{record['scaled']}"}


def step_4_aggregate(dataset: Any, *, context: object) -> dict[str, Any]:
    """Aggregate every committed partition behind the barrier."""
    _ = context
    records = {partition.partition_id: partition.load() for partition in dataset.partitions()}
    return {
        "pipeline": PIPELINE_NAME,
        "count": len(records),
        "ids": sorted(records),
        "total_scaled": sum(int(record["scaled"]) for record in records.values()),
    }


def build_persist_only_final_pipeline() -> Pipeline:
    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(
                name=STAGE_NAME,
                entries=[
                    step_1_collect,
                    step_2_scale,
                    step_3_label,
                    Step(
                        step_4_aggregate,
                        output=JSONDataset(name="aggregate"),
                        input_scope="all",
                    ),
                ],
            )
        ],
        source=CallableLoader("partitions", lambda: PARTITIONS),
    )


def run_persist_only_final_pipeline(
    *, state_root: Path | str, job_id: str = "persist-only-final-1"
) -> Any:
    return run(
        build_persist_only_final_pipeline(),
        job_id=job_id,
        storage=DurableStorage.at(state_root),
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
