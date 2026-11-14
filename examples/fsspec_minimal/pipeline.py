"""Local durable metadata with remote-capable artifact storage.

This is the supported storage envelope. The authoritative progress store needs
a local filesystem that survives invocations, because it is the thing that must
provide transactions and fencing. Committed artifacts have no such constraint:
they are configured and validated independently and may live in object storage.

Set ``CARAVEL_ARTIFACTS_ROOT`` to an fsspec URL (``s3://``, ``az://``,
``memory://``) to exercise remote artifacts. Durable state stays local.
"""

from __future__ import annotations

import os
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

PIPELINE_NAME = "fsspec_minimal_example"
BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"

BRONZE_STEP_NAME = "bronze_collect"
SILVER_TRANSFORM_STEP_NAME = "silver_transform"
SILVER_FINALIZE_STEP_NAME = "silver_finalize"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def _runtime_params(context: object) -> Mapping[str, str]:
    params = getattr(context, "params", {})
    if isinstance(params, dict):
        return {str(key): str(value) for key, value in params.items()}
    return {}


def bronze_collect(inputs, *, context: object) -> dict[str, Any]:
    """Add bronze fields to one execution partition's record."""
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
    """Normalize one bronze record."""
    records = {}
    for partition in inputs.partitions():
        for key, record in partition.load().items():
            if key in records:
                raise ValueError(f"Duplicate record key: {key}")
            records[key] = {**record, "normalized": float(record["doubled_value"]) / 4.0}
    return records


def silver_finalize(inputs, *, context: object) -> dict[str, Any]:
    """Aggregate every committed partition behind an all-input barrier."""
    params = _runtime_params(context)
    records = {}
    for partition in inputs.partitions():
        for key, record in partition.load().items():
            if key in records:
                raise ValueError(f"Duplicate record key: {key}")
            records[key] = record
    ordered_keys = sorted(records)
    payload: dict[str, Any] = {
        "pipeline": PIPELINE_NAME,
        "count": len(records),
        "ids": ordered_keys,
        "total_normalized": sum(float(records[key]["normalized"]) for key in ordered_keys),
        "records": {key: records[key] for key in ordered_keys},
    }
    mode = params.get("mode")
    if mode is not None:
        payload["mode"] = mode
    return payload


def build_fsspec_pipeline(
    input_path: Path | str | None = None,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> Pipeline:
    """Build the declaration. Input may itself be a local path or fsspec URL."""
    seed = input_path if input_path is not None else _DEFAULT_INPUT_PATH
    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(
                name=BRONZE_STAGE_NAME,
                entries=[Step(bronze_collect, output=JSONDataset(), revision="2")],
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[
                    Step(silver_transform, output=JSONDataset(), revision="2"),
                    Step(
                        silver_finalize,
                        input_scope="all",
                        output=JSONDataset(name="finalize"),
                    ),
                ],
            ),
        ],
        source=dataset_as_loader(
            JSONDataset(name="fsspec_seed", path=seed, storage_options=storage_options)
        ),
    )


def build_fsspec_storage(
    *,
    state_root: Path | str,
    artifacts_root: str | Path | None = None,
    storage_options: Mapping[str, Any] | None = None,
) -> DurableStorage:
    """Local authoritative state; artifacts wherever they are configured."""
    return DurableStorage.at(
        Path(state_root) / "output",
        artifacts_root=artifacts_root or os.getenv("CARAVEL_ARTIFACTS_ROOT"),
        storage_options=storage_options,
    )


def run_fsspec_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "fsspec-minimal-1",
    input_path: Path | str | None = None,
    artifacts_root: str | Path | None = None,
    storage_options: Mapping[str, Any] | None = None,
    params: dict[str, str] | None = None,
) -> Any:
    """Run the example and return its structured status."""
    pipeline = build_fsspec_pipeline(input_path=input_path, storage_options=storage_options)
    storage = build_fsspec_storage(
        state_root=state_root,
        artifacts_root=artifacts_root,
        storage_options=storage_options,
    )
    return run(pipeline, job_id=job_id, storage=storage, params=params)


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_TRANSFORM_STEP_NAME",
    "SILVER_FINALIZE_STEP_NAME",
    "build_fsspec_pipeline",
    "build_fsspec_storage",
    "run_fsspec_pipeline",
]
