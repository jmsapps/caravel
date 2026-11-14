"""Local durable metadata with remote-capable artifact storage.

This is the supported storage envelope. The authoritative progress store needs
a local filesystem that survives invocations, because it is the thing that must
provide transactions and fencing. Committed artifacts have no such constraint:
they are configured and validated independently and may live in object storage.

Set ``CARAVEL_ARTIFACTS_ROOT`` to an fsspec URL (``s3://``, ``az://``,
``memory://``) to exercise remote artifacts. Durable state stays local.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, TypedDict

from caravel import (
    StepContext,
    RunResult,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
)

from ..support import durable

PIPELINE_NAME = "fsspec_minimal_example"
BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"

BRONZE_STEP_NAME = "bronze_collect"
SILVER_TRANSFORM_STEP_NAME = "silver_transform"
SILVER_FINALIZE_STEP_NAME = "silver_finalize"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def _runtime_params(context: StepContext) -> Mapping[str, str]:
    """The run-scoped parameters this invocation was given."""
    return context.params


class SeedRecord(TypedDict):
    """One record as the seed dataset supplies it."""

    id: str
    value: int


class BronzeRecord(SeedRecord):
    """A seed record after the bronze step."""

    bronze: bool
    doubled_value: int


class SilverRecord(BronzeRecord):
    """A bronze record after the silver step."""

    normalized: float


SeedRecords = dict[str, SeedRecord]
BronzeRecords = dict[str, BronzeRecord]
SilverRecords = dict[str, SilverRecord]


def bronze_collect(value: SeedRecords, *, context: StepContext) -> BronzeRecords:
    """Add bronze fields to every incoming record."""
    return {
        key: BronzeRecord(**record, bronze=True, doubled_value=int(record["value"]) * 2)
        for key, record in value.items()
    }


def silver_transform(value: BronzeRecords, *, context: StepContext) -> SilverRecords:
    """Normalize every bronze record."""
    return {
        key: SilverRecord(**record, normalized=float(record["doubled_value"]) / 4.0)
        for key, record in value.items()
    }


def silver_finalize(value: SilverRecords, *, context: StepContext) -> dict[str, Any]:
    """Aggregate every committed partition behind an all-input barrier."""
    params = _runtime_params(context)
    records = {}
    for key, record in value.items():
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
                entries=[Step(bronze_collect, output=JSONDataset())],
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[
                    Step(silver_transform, output=JSONDataset()),
                    Step(
                        silver_finalize,
                        output=JSONDataset(name="finalize"),
                    ),
                ],
            ),
        ],
        source=dataset_as_loader(
            JSONDataset(name="fsspec_seed", path=seed, storage_options=storage_options)
        ),
    )


def build_fsspec_output(*, state_root: Path | str) -> Path:
    """Where this example's generated output goes."""
    return Path(state_root) / "output"


def run_fsspec_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "fsspec-minimal-1",
    input_path: Path | str | None = None,
    artifacts_root: str | Path | None = None,
    storage_options: Mapping[str, Any] | None = None,
    params: dict[str, str] | None = None,
) -> RunResult:
    """Run the example and return its structured status."""
    pipeline = build_fsspec_pipeline(input_path=input_path, storage_options=storage_options)
    return run(
        durable(pipeline, job_id),
        output=build_fsspec_output(state_root=state_root),
        params=params,
    )


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_TRANSFORM_STEP_NAME",
    "SILVER_FINALIZE_STEP_NAME",
    "build_fsspec_pipeline",
    "build_fsspec_output",
    "run_fsspec_pipeline",
]
