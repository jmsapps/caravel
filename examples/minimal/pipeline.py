from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
    DurableStorage,
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
)

PIPELINE_NAME = "minimal_example"
BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"

BRONZE_STEP_NAME = "bronze_collect"
SILVER_TRANSFORM_STEP_NAME = "silver_transform"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def bronze_collect(inputs, *, context: object) -> dict[str, Any]:
    """Add bronze fields to the available records."""
    records = {}
    for partition in inputs.partitions():
        record = partition.load()
        records[partition.partition_id] = {
            **record,
            "bronze": True,
            "doubled_value": int(record["value"]) * 2,
        }
    return records


def silver_transform(inputs, *, context: object) -> dict[str, Any]:
    """Normalize available bronze records without assuming one input handle."""
    records = {}
    for partition in inputs.partitions():
        for key, record in partition.load().items():
            if key in records:
                raise ValueError(f"Duplicate record key: {key}")
            records[key] = {**record, "normalized": float(record["doubled_value"]) / 4.0}
    return records


def build_minimal_pipeline(input_path: Path | str | None = None) -> Pipeline:
    """Build the minimal example pipeline declaration."""
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH

    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(
                name=BRONZE_STAGE_NAME,
                entries=[Step(bronze_collect, output=PartitionedJSONDataset())],
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[
                    Step(silver_transform, output=PartitionedJSONDataset()),
                ],
            ),
        ],
        # A loader describes input; it is not a separate execution path.
        source=dataset_as_loader(JSONDataset(name="minimal_seed", path=seed_path)),
    )


def run_minimal_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "minimal-1",
    input_path: Path | str | None = None,
    params: dict[str, str] | None = None,
) -> Any:
    """Run the minimal example and return its structured status."""
    pipeline = build_minimal_pipeline(input_path=input_path)
    return run(
        pipeline,
        job_id=job_id,
        storage=DurableStorage.at(Path(state_root) / "output"),
        params=params,
    )


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_TRANSFORM_STEP_NAME",
    "build_minimal_pipeline",
    "run_minimal_pipeline",
]
