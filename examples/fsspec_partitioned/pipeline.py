"""Partitioned output with remote-capable artifact storage.

Same storage envelope as ``fsspec_minimal``: the authoritative progress store
is local, and committed artifacts may live in object storage.

The retired runner let each stage pick its own output root (``stage_root``).
Durable execution owns artifact placement, because committed output must be
reachable from the manifest; one ``artifacts_root`` replaces the per-stage
roots.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from caravel import (
    DurableStorage,
    JSONDataset,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
    step,
)

PIPELINE_NAME = "fsspec_partitioned_example"
BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"
GOLD_STAGE_NAME = "gold"

BRONZE_STEP_NAME = "bronze_render_html"
SILVER_STEP_NAME = "silver_extract_structured"
GOLD_STEP_NAME = "gold_summarize"

_DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "data" / "input_partitions"


def _extract_between(value: str, start: str, end: str) -> str:
    start_idx = value.index(start) + len(start)
    return value[start_idx : value.index(end, start_idx)]


@step(output=PartitionedTextDataset(suffix=".html", encoding="utf-8"))
def bronze_render_html(record: dict[str, Any], *, context: object) -> dict[str, str]:
    """Render one source record into a deterministic HTML partition."""
    _ = context
    item_id = str(record["id"])
    return {
        item_id: (
            f"<article data-id='{item_id}' data-lang='{record['lang']}'>"
            f"<h1>{record['title']}</h1>"
            f"<p>{record['snippet']}</p>"
            "</article>"
        )
    }


@step(output=PartitionedJSONDataset())
def silver_extract_structured(
    payload: dict[str, str], *, context: object
) -> dict[str, dict[str, Any]]:
    """Convert one partition's bronze HTML back into structured records."""
    _ = context
    structured: dict[str, dict[str, Any]] = {}
    for key in sorted(payload):
        html = payload[key]
        snippet = _extract_between(html, "<p>", "</p>")
        structured[key] = {
            "id": _extract_between(html, "data-id='", "'"),
            "lang": _extract_between(html, "data-lang='", "'"),
            "title": _extract_between(html, "<h1>", "</h1>"),
            "snippet": snippet,
            "normalized_snippet": snippet.lower(),
        }
    return structured


def gold_summarize(dataset: Any, *, context: object) -> dict[str, Any]:
    """Summarize every committed partition behind an all-input barrier."""
    _ = context
    languages: dict[str, int] = {}
    total = 0
    for partition in dataset.partitions():
        for record in partition.load().values():
            languages[str(record["lang"])] = languages.get(str(record["lang"]), 0) + 1
            total += 1
    return {
        "pipeline": PIPELINE_NAME,
        "total_records": total,
        "counts_by_language": dict(sorted(languages.items())),
    }


def build_fsspec_partitioned_pipeline(
    input_dir: Path | str | None = None,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> Pipeline:
    seed = input_dir if input_dir is not None else _DEFAULT_INPUT_DIR
    return Pipeline(
        name=PIPELINE_NAME,
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[bronze_render_html]),
            Stage(name=SILVER_STAGE_NAME, entries=[silver_extract_structured]),
            Stage(
                name=GOLD_STAGE_NAME,
                entries=[
                    Step(
                        gold_summarize,
                        output=JSONDataset(name="summary"),
                        input_scope="all",
                    )
                ],
            ),
        ],
        source=dataset_as_loader(
            PartitionedJSONDataset(
                name="fsspec_partitioned_seed",
                path=seed,
                storage_options=storage_options,
            )
        ),
    )


def run_fsspec_partitioned_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "fsspec-partitioned-1",
    input_dir: Path | str | None = None,
    artifacts_root: str | Path | None = None,
    storage_options: Mapping[str, Any] | None = None,
) -> Any:
    """Run the example and return its structured status."""
    pipeline = build_fsspec_partitioned_pipeline(
        input_dir=input_dir, storage_options=storage_options
    )
    storage = DurableStorage.at(
        state_root,
        artifacts_root=artifacts_root or os.getenv("CARAVEL_ARTIFACTS_ROOT"),
        storage_options=storage_options,
    )
    return run(pipeline, job_id=job_id, storage=storage)


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "GOLD_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_STEP_NAME",
    "GOLD_STEP_NAME",
    "build_fsspec_partitioned_pipeline",
    "run_fsspec_partitioned_pipeline",
]
