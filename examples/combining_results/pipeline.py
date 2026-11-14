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

PIPELINE_NAME = "combining_results_example"
_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def transform(inputs, *, context: object) -> dict[str, Any]:
    """Process each partition independently before combining results."""
    records = {}
    for partition in inputs.partitions():
        record = partition.load()
        records[partition.partition_id] = {**record, "normalized": float(record["value"]) / 2.0}
    return records


def combine_results(inputs, *, context: object) -> dict[str, Any]:
    """Read every committed partition and return one combined summary."""
    records = {}
    for partition in inputs.partitions():
        for key, record in partition.load().items():
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
                entries=[Step(combine_results, input_scope="all", output=JSONDataset())],
            ),
        ],
    )


def run_combining_pipeline(
    *, state_root: Path | str, job_id: str = "combining-1", input_path: Path | str | None = None
) -> Any:
    with DurableStorage.at(Path(state_root) / "output") as storage:
        return run(build_combining_pipeline(input_path), job_id=job_id, storage=storage)
