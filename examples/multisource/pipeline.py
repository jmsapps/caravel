from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline import (
    SOURCE_FIELD,
    JSONDataset,
    MultiSourceLoader,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    run,
    step,
)

PIPELINE_NAME = "multisource_example"
STAGE_NAME = "silver"
STEP_NAME = "summarize_by_source"

SOURCE_A_NAME = "src_a"
SOURCE_B_NAME = "src_b"

_DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"
_DEFAULT_SOURCE_A_DIR = _DEFAULT_DATA_DIR / SOURCE_A_NAME
_DEFAULT_SOURCE_B_DIR = _DEFAULT_DATA_DIR / SOURCE_B_NAME

_DEFAULT_COLLISION_DIR = Path(__file__).resolve().parent / "data_collision"
_DEFAULT_COLLISION_SOURCE_A_DIR = _DEFAULT_COLLISION_DIR / SOURCE_A_NAME
_DEFAULT_COLLISION_SOURCE_B_DIR = _DEFAULT_COLLISION_DIR / SOURCE_B_NAME


def make_multisource_loader(
    *,
    source_a_dir: Path | str | None = None,
    source_b_dir: Path | str | None = None,
) -> MultiSourceLoader:
    """Build the default multisource loader for happy-path fixtures."""
    resolved_a = Path(source_a_dir) if source_a_dir is not None else _DEFAULT_SOURCE_A_DIR
    resolved_b = Path(source_b_dir) if source_b_dir is not None else _DEFAULT_SOURCE_B_DIR

    return MultiSourceLoader(
        [
            PartitionedJSONDataset(name=SOURCE_A_NAME, path=resolved_a),
            PartitionedJSONDataset(name=SOURCE_B_NAME, path=resolved_b),
        ]
    )


@step(output=JSONDataset())
def summarize_by_source(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, Any]:
    """Validate source tags and emit deterministic counts by source."""
    _ = context
    counts_by_source: dict[str, int] = {SOURCE_A_NAME: 0, SOURCE_B_NAME: 0}

    for key in sorted(partitions):
        record = partitions[key]
        if not isinstance(record, dict):
            raise TypeError(
                f"Expected dict record for partition '{key}', got {type(record).__name__}."
            )

        if SOURCE_FIELD not in record:
            raise KeyError(f"Partition '{key}' is missing source tag '{SOURCE_FIELD}'.")

        source_name = str(record[SOURCE_FIELD])
        if source_name not in counts_by_source:
            raise ValueError(f"Unexpected source tag '{source_name}' for partition '{key}'.")

        counts_by_source[source_name] += 1

    total_records = sum(counts_by_source.values())

    return {
        "pipeline": PIPELINE_NAME,
        "counts_by_source": counts_by_source,
        "total_records": total_records,
    }


def build_multisource_pipeline(
    *,
    source_a_dir: Path | str | None = None,
    source_b_dir: Path | str | None = None,
) -> Pipeline:
    """Build the multisource pipeline declaration."""
    loader = make_multisource_loader(source_a_dir=source_a_dir, source_b_dir=source_b_dir)

    return Pipeline(
        name=PIPELINE_NAME,
        loader=loader,
        stages=[Stage(name=STAGE_NAME, entries=[summarize_by_source])],
    )


def run_multisource_pipeline(
    *,
    run_root: Path | str | None = None,
    source_a_dir: Path | str | None = None,
    source_b_dir: Path | str | None = None,
) -> Path:
    """Run the multisource example pipeline and return resolved run root."""
    pipeline = build_multisource_pipeline(source_a_dir=source_a_dir, source_b_dir=source_b_dir)
    return run(pipeline, run_root=run_root)


__all__ = [
    "PIPELINE_NAME",
    "STAGE_NAME",
    "STEP_NAME",
    "SOURCE_A_NAME",
    "SOURCE_B_NAME",
    "_DEFAULT_SOURCE_A_DIR",
    "_DEFAULT_SOURCE_B_DIR",
    "_DEFAULT_COLLISION_SOURCE_A_DIR",
    "_DEFAULT_COLLISION_SOURCE_B_DIR",
    "make_multisource_loader",
    "build_multisource_pipeline",
    "run_multisource_pipeline",
]
