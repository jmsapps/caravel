"""Every step commits; aggregation is an explicit barrier.

The legacy ``persist=False`` option is gone: durable recovery requires each
step's output to be committed so a later invocation restores it instead of
recomputing. What used to be "only the last step persists" is now expressed as
cheap per-partition steps feeding one ```` aggregation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
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

PARTITIONS: dict[str, dict[str, Any]] = {
    f"record-{index}": {"id": f"record-{index}", "value": index} for index in range(1, 5)
}


def step_1_collect(value, *, context: object) -> dict[str, Any]:
    records = {}
    for _key, _record in value.items():
        records[_key] = {**_record, "collected": True}
    return records


def step_2_scale(value, *, context: object) -> dict[str, Any]:
    records = {}
    for key, record in value.items():
        if key in records:
            raise ValueError(f"Duplicate record key: {key}")
        records[key] = {**record, "scaled": int(record["value"]) * 10}
    return records


def step_3_label(value, *, context: object) -> dict[str, Any]:
    records = {}
    for key, record in value.items():
        if key in records:
            raise ValueError(f"Duplicate record key: {key}")
        records[key] = {**record, "label": f"v{record['scaled']}"}
    return records


def step_4_aggregate(value, *, context: object) -> dict[str, Any]:
    """Aggregate every committed partition behind the barrier."""
    _ = context
    records = {}
    for key, record in value.items():
        if key in records:
            raise ValueError(f"Duplicate record key: {key}")
        records[key] = record
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
) -> Any:
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
