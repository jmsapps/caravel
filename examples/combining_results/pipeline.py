from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import DurableStorage, JSONDataset, Pipeline, Stage, Step, dataset_as_loader, run

PIPELINE_NAME = "combining_results_example"
_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def transform_partition(partition, *, context: object) -> dict[str, Any]:
    """Process each partition independently before combining results."""
    record: dict[str, Any] = partition.load()
    return {**record, "normalized": float(record["value"]) / 2.0}


def transform(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(transform_partition)


def combine_results_all(partitions, *, context: object) -> dict[str, Any]:
    """Read all committed partitions and return one combined summary."""
    dataset = partitions
    records = {partition.partition_id: partition.load() for partition in dataset.partitions()}
    ids = sorted(records)
    return {
        "pipeline": PIPELINE_NAME,
        "count": len(records),
        "ids": ids,
        "total_normalized": sum(float(records[key]["normalized"]) for key in ids),
        "records": {key: records[key] for key in ids},
    }


def combine_results(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.all(combine_results_all)


def build_combining_pipeline(input_path: Path | str | None = None) -> Pipeline:
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH
    return Pipeline(
        name=PIPELINE_NAME,
        source=dataset_as_loader(JSONDataset(name="seed", path=seed_path)),
        stages=[
            Stage(name="prepare", entries=[Step(transform, output=JSONDataset())]),
            Stage(
                name="combine",
                entries=[Step(combine_results, output=JSONDataset())],
            ),
        ],
    )


def run_combining_pipeline(
    *, state_root: Path | str, job_id: str = "combining-1", input_path: Path | str | None = None
) -> Any:
    with DurableStorage.at(Path(state_root) / "output") as storage:
        return run(build_combining_pipeline(input_path), job_id=job_id, storage=storage)
