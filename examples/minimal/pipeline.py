from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from caravel import (
    DurableStorage,
    JSONDataset,
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
SILVER_FINALIZE_STEP_NAME = "silver_finalize"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def _runtime_params(context: object) -> Mapping[str, str]:
    """Return run-scoped params from step context when available."""
    params = getattr(context, "params", {})
    if isinstance(params, dict):
        return {str(key): str(value) for key, value in params.items()}
    return {}


def bronze_collect_partition(partition, *, context: object) -> dict[str, Any]:
    """Add deterministic bronze fields. Runs once per execution partition."""
    record: dict[str, Any] = partition.load()
    _ = context
    return {**record, "bronze": True, "doubled_value": int(record["value"]) * 2}


def bronze_collect(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(bronze_collect_partition)


def silver_transform_partition(partition, *, context: object) -> dict[str, Any]:
    """Normalize one bronze record. Partition identity is preserved by the engine."""
    record: dict[str, Any] = partition.load()
    _ = context
    return {**record, "normalized": float(record["doubled_value"]) / 4.0}


def silver_transform(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(silver_transform_partition)


def silver_finalize_all(partitions, *, context: object) -> dict[str, Any]:
    """Summarize the whole run behind an explicit all-input barrier.

    Aggregation is the one thing that genuinely needs every partition, so it is
    declared with ``inputs.all(...)``. It receives a reader over committed
    partitions rather than a materialized dictionary.
    """
    dataset = partitions
    params = _runtime_params(context)
    mode = params.get("mode")
    records: dict[str, dict[str, Any]] = {
        partition.partition_id: partition.load() for partition in dataset.partitions()
    }
    ordered_keys = sorted(records)
    records = {key: records[key] for key in ordered_keys}
    total_normalized = sum(float(record["normalized"]) for record in records.values())

    payload: dict[str, Any] = {
        "pipeline": PIPELINE_NAME,
        "count": len(records),
        "ids": ordered_keys,
        "total_normalized": total_normalized,
        "records": records,
    }

    if mode is not None:
        payload["mode"] = mode

    return payload


def silver_finalize(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.all(silver_finalize_all)


def build_minimal_pipeline(input_path: Path | str | None = None) -> Pipeline:
    """Build the minimal example pipeline declaration."""
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH

    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[Step(bronze_collect, output=JSONDataset())]),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[
                    Step(silver_transform, output=JSONDataset()),
                    Step(silver_finalize, output=JSONDataset()),
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
    "SILVER_FINALIZE_STEP_NAME",
    "build_minimal_pipeline",
    "run_minimal_pipeline",
]
