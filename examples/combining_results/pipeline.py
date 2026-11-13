from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import DurableStorage, JSONDataset, Pipeline, Stage, Step, dataset_as_loader, run

PIPELINE_NAME = "combining_results_example"
_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"


def transform(record: dict[str, Any], *, context: object) -> dict[str, Any]:
    """Process each partition independently before combining results."""
    return {**record, "normalized": float(record["value"]) / 2.0}


def combine_results(dataset: Any, *, context: object) -> dict[str, Any]:
    """Read all committed partitions and return one combined summary."""
    records = {partition.partition_id: partition.load() for partition in dataset.partitions()}
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
            Stage(name="prepare", entries=[Step(transform, output=JSONDataset())]),
            Stage(
                name="combine",
                entries=[Step(combine_results, output=JSONDataset(), input_scope="all")],
            ),
        ],
    )


def run_combining_pipeline(
    *, state_root: Path | str, job_id: str = "combining-1", input_path: Path | str | None = None
) -> Any:
    with DurableStorage.at(Path(state_root) / "output") as storage:
        return run(build_combining_pipeline(input_path), job_id=job_id, storage=storage)
