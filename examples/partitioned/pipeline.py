from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
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
def bronze_render_html(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, str]:
    """Render source JSON partitions into deterministic HTML partitions."""
    _ = context
    rendered: dict[str, str] = {}

    for key in sorted(partitions):
        record = partitions[key]
        item_id = str(record["id"])
        lang = str(record["lang"])
        title = str(record["title"])
        snippet = str(record["snippet"])
        rendered[key] = (
            f"<article data-id='{item_id}' data-lang='{lang}'>"
            f"<h1>{title}</h1>"
            f"<p>{snippet}</p>"
            "</article>"
        )

    return rendered


@step(output=PartitionedJSONDataset())
def silver_extract_structured(
    partitions: dict[str, str], *, context: object
) -> dict[str, dict[str, Any]]:
    """Convert bronze HTML partitions back into structured JSON records."""
    _ = context
    structured: dict[str, dict[str, Any]] = {}

    for key in sorted(partitions):
        html = partitions[key]
        item_id = _extract_between(html, "data-id='", "'")
        lang = _extract_between(html, "data-lang='", "'")
        title = _extract_between(html, "<h1>", "</h1>")
        snippet = _extract_between(html, "<p>", "</p>")

        structured[key] = {
            "id": item_id,
            "lang": lang,
            "title": title,
            "snippet": snippet,
            "normalized_snippet": snippet.lower(),
            "source_key": key,
        }

    return structured


@step(output=PartitionedJSONDataset())
def gold_partition_by_language(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Emit path-style keys (`<lang>/<id>`) to prove nested partition output paths."""
    _ = context
    repartitioned: dict[str, dict[str, Any]] = {}

    for key in sorted(partitions):
        record = partitions[key]
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

    loader = dataset_as_loader(PartitionedJSONDataset(name="partitioned_seed", path=seed_dir))

    return Pipeline(
        name=PIPELINE_NAME,
        loader=loader,
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[bronze_render_html]),
            Stage(name=SILVER_STAGE_NAME, entries=[silver_extract_structured]),
            Stage(name=GOLD_STAGE_NAME, entries=[gold_partition_by_language]),
        ],
    )


def run_partitioned_pipeline(
    *,
    run_root: Path | str | None = None,
    input_dir: Path | str | None = None,
) -> Path | str:
    """Run the partitioned example pipeline and return resolved run root."""
    pipeline = build_partitioned_pipeline(input_dir=input_dir)
    return run(pipeline, run_root=run_root)


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
