from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import JSONDataset, Pipeline, Stage, dataset_as_loader, run, step

PIPELINE_NAME = "persist_only_final_example"
STAGE_NAME = "single_stage"

STEP_1_NAME = "step_1_collect"
STEP_2_NAME = "step_2_scale"
STEP_3_NAME = "step_3_tag"
STEP_4_NAME = "step_4_aggregate"
STEP_5_NAME = "step_5_finalize"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input" / "input_partitions.json"


@step(output=JSONDataset(), persist=False)
def step_1_collect(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    _ = context
    return {k: {**v, "seen": True} for k, v in partitions.items()}


@step(output=JSONDataset(), persist=False)
def step_2_scale(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    _ = context
    return {k: {**v, "scaled": int(v["value"]) * 10} for k, v in partitions.items()}


@step(output=JSONDataset(), persist=False)
def step_3_tag(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    _ = context
    return {k: {**v, "tag": f"row-{k}"} for k, v in partitions.items()}


@step(output=JSONDataset(), persist=False)
def step_4_aggregate(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, Any]:
    _ = context
    return {
        "count": len(partitions),
        "total_scaled": sum(int(v["scaled"]) for v in partitions.values()),
        "records": partitions,
    }


@step(output=JSONDataset())
def step_5_finalize(payload: dict[str, Any], *, context: object) -> dict[str, Any]:
    _ = context
    records = payload["records"]
    return {
        "count": payload["count"],
        "total_scaled": payload["total_scaled"],
        "ids": sorted(records.keys()),
        "records": records,
    }


def build_persist_only_final_pipeline(input_path: Path | str | None = None) -> Pipeline:
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH
    loader = dataset_as_loader(JSONDataset(name="persist_only_final_seed", path=seed_path))
    return Pipeline(
        name=PIPELINE_NAME,
        loader=loader,
        stages=[
            Stage(
                name=STAGE_NAME,
                entries=[step_1_collect, step_2_scale, step_3_tag, step_4_aggregate, step_5_finalize],
            )
        ],
    )


def run_persist_only_final_pipeline(
    *, run_root: Path | str | None = None, input_path: Path | str | None = None
) -> Path | str:
    pipeline = build_persist_only_final_pipeline(input_path=input_path)
    resolved_root = (
        Path(run_root)
        if run_root is not None
        else Path(__file__).resolve() / "data" / "output"
    )
    return run(pipeline, run_root=resolved_root)


__all__ = [
    "PIPELINE_NAME",
    "STAGE_NAME",
    "STEP_1_NAME",
    "STEP_2_NAME",
    "STEP_3_NAME",
    "STEP_4_NAME",
    "STEP_5_NAME",
    "build_persist_only_final_pipeline",
    "run_persist_only_final_pipeline",
]
