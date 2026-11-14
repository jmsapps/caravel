from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from caravel import (
    RunResult,
    StepContext,
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
)

from ..support import durable

PIPELINE_NAME = "combining_results_example"
_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


class SeedRecord(TypedDict):
    """One record as the seed dataset supplies it."""

    id: str
    value: int


class NormalizedRecord(SeedRecord):
    """A seed record after the transform step."""

    normalized: float


class Summary(TypedDict):
    """The combined view of every normalized record."""

    pipeline: str
    count: int
    ids: list[str]
    total_normalized: float
    records: dict[str, NormalizedRecord]


SeedRecords = dict[str, SeedRecord]
NormalizedRecords = dict[str, NormalizedRecord]


def transform(value: SeedRecords, *, context: StepContext) -> NormalizedRecords:
    """Process every incoming record before combining them."""
    return {
        key: NormalizedRecord(**record, normalized=float(record["value"]) / 2.0)
        for key, record in value.items()
    }


def combine_results(value: NormalizedRecords, *, context: StepContext) -> Summary:
    """Read every normalized record and return one combined summary."""
    ids = sorted(value)
    return Summary(
        pipeline=PIPELINE_NAME,
        count=len(value),
        ids=ids,
        total_normalized=sum(float(value[key]["normalized"]) for key in ids),
        records={key: value[key] for key in ids},
    )


def build_combining_pipeline(input_path: Path | str | None = None) -> Pipeline:
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH
    return Pipeline(
        name=PIPELINE_NAME,
        source=dataset_as_loader(JSONDataset(name="seed", path=seed_path)),
        stages=[
            Stage(name="prepare", entries=[Step(transform, output=PartitionedJSONDataset())]),
            Stage(
                name="combine",
                entries=[Step(combine_results, output=JSONDataset())],
            ),
        ],
    )


def run_combining_pipeline(
    *, state_root: Path | str, job_id: str = "combining-1", input_path: Path | str | None = None
) -> RunResult:
    return run(
        durable(build_combining_pipeline(input_path), job_id),
        output=Path(state_root) / "output",
    )
