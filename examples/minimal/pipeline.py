from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from pipeline import JSONDataset, Pipeline, Stage, dataset_as_loader, run, step

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


@step(output=JSONDataset())
def bronze_collect(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Add deterministic bronze fields to each incoming record."""
    _ = context
    transformed: dict[str, dict[str, Any]] = {}
    for key in sorted(partitions):
        record = partitions[key]
        transformed[key] = {
            **record,
            "bronze": True,
            "doubled_value": int(record["value"]) * 2,
        }
    return transformed


@step(output=JSONDataset())
def silver_transform(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize bronze records while preserving partition keys."""
    _ = context
    normalized: dict[str, dict[str, Any]] = {}
    for key in sorted(partitions):
        record = partitions[key]
        normalized[key] = {
            **record,
            "normalized": float(record["doubled_value"]) / 4.0,
        }
    return normalized


@step(output=JSONDataset())
def silver_finalize(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, Any]:
    """Produce deterministic final summary payload for smoke testing."""
    params = _runtime_params(context)
    mode = params.get("mode")
    ordered_keys = sorted(partitions)
    records: dict[str, dict[str, Any]] = {key: partitions[key] for key in ordered_keys}
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


def build_minimal_pipeline(input_path: Path | str | None = None) -> Pipeline:
    """Build the minimal example pipeline declaration."""
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH

    loader = dataset_as_loader(JSONDataset(name="minimal_seed", path=seed_path))

    return Pipeline(
        name=PIPELINE_NAME,
        loader=loader,
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[bronze_collect]),
            Stage(name=SILVER_STAGE_NAME, entries=[silver_transform, silver_finalize]),
        ],
    )


def run_minimal_pipeline(
    *,
    run_root: Path | str | None = None,
    input_path: Path | str | None = None,
) -> Path:
    """Run the minimal example pipeline and return the resolved run root."""
    pipeline = build_minimal_pipeline(input_path=input_path)
    return run(pipeline, run_root=run_root)


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
