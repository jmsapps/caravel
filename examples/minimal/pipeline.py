from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from caravel import (
    RunResult,
    DurabilityOptions,
    StepContext,
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
        key: BronzeRecord(
            id=record["id"],
            value=record["value"],
            bronze=True,
            doubled_value=int(record["value"]) * 2,
        )
        for key, record in value.items()
    }


def silver_transform(value: BronzeRecords, *, context: StepContext) -> SilverRecords:
    """Normalize every bronze record."""
    return {
        key: SilverRecord(**record, normalized=float(record["doubled_value"]) / 4.0)
        for key, record in value.items()
    }


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
) -> RunResult:
    """Run the minimal example durably and return its result."""
    pipeline = build_minimal_pipeline(input_path=input_path)
    pipeline.durability_options = DurabilityOptions(
        schema_version="v1.0.0", job_id=job_id, commit_scope="step"
    )
    return run(pipeline, output=Path(state_root) / "output", params=params)


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_TRANSFORM_STEP_NAME",
    "build_minimal_pipeline",
    "run_minimal_pipeline",
]
