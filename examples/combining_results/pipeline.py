from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
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


def transform(value, *, context: object) -> dict[str, Any]:
    """Process each partition independently before combining results."""
    records = {}
    for _key, record in value.items():
        records[_key] = {**record, "normalized": float(record["value"]) / 2.0}
    return records


def combine_results(value, *, context: object) -> dict[str, Any]:
    """Read every committed partition and return one combined summary."""
    records = {}
    for key, record in value.items():
        if key in records:
            raise ValueError(f"Duplicate record key: {key}")
        records[key] = record
    ids = sorted(records)
    return {
        "pipeline": PIPELINE_NAME,
        "count": len(records),
        "ids": ids,
        "total_normalized": sum(float(records[key]["normalized"]) for key in ids),
        "records": {key: records[key] for key in ids},
    }


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
) -> Any:
    return run(
        durable(build_combining_pipeline(input_path), job_id),
        output=Path(state_root) / "output",
    )
