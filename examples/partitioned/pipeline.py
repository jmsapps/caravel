from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
    DurableStorage,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    dataset_as_loader,
    run,
    step,
)

PIPELINE_NAME = "partitioned_example"

BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"
GOLD_STAGE_NAME = "gold"

BRONZE_STEP_NAME = "bronze_render_html"
SILVER_STEP_NAME = "silver_extract_structured"
GOLD_STEP_NAME = "gold_partition_by_language"

_DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "data" / "input_partitions"


def _extract_between(value: str, start: str, end: str) -> str:
    """Extract deterministic substring boundaries from generated bronze HTML."""
    start_idx = value.index(start) + len(start)
    end_idx = value.index(end, start_idx)
    return value[start_idx:end_idx]


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
    """Convert one partition's bronze HTML back into a structured record."""
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
            "source_key": key,
        }
    return structured


@step(output=PartitionedJSONDataset())
def gold_partition_by_language(
    payload: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Emit path-style keys (`<lang>/<id>`) to prove nested storage keys.

    These are *storage* partition keys inside one payload. They occupy a
    different namespace from execution partition identity, which the engine
    keeps stable across steps.
    """
    _ = context
    repartitioned: dict[str, dict[str, Any]] = {}
    for key in sorted(payload):
        record = payload[key]
        lang = str(record["lang"])
        item_id = str(record["id"])
        repartitioned[f"{lang}/{item_id}"] = {
            "id": item_id,
            "lang": lang,
            "title": record["title"],
            "normalized_snippet": record["normalized_snippet"],
            "origin_partition": key,
        }
    return repartitioned


def build_partitioned_pipeline(input_dir: Path | str | None = None) -> Pipeline:
    """Build the partitioned pipeline declaration."""
    seed_dir = Path(input_dir) if input_dir is not None else _DEFAULT_INPUT_DIR

    return Pipeline(
        name=PIPELINE_NAME,
        source=dataset_as_loader(PartitionedJSONDataset(name="partitioned_seed", path=seed_dir)),
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[bronze_render_html]),
            Stage(name=SILVER_STAGE_NAME, entries=[silver_extract_structured]),
            Stage(name=GOLD_STAGE_NAME, entries=[gold_partition_by_language]),
        ],
    )


def run_partitioned_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "partitioned-1",
    input_dir: Path | str | None = None,
) -> Any:
    """Run the partitioned example and return its structured status."""
    pipeline = build_partitioned_pipeline(input_dir=input_dir)
    return run(pipeline, job_id=job_id, storage=DurableStorage.at(state_root))


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "GOLD_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_STEP_NAME",
    "GOLD_STEP_NAME",
    "build_partitioned_pipeline",
    "run_partitioned_pipeline",
]
