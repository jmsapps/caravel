from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from caravel import (
    RunResult,
    StepContext,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
)

from ..support import durable

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


class SourceRecord(TypedDict):
    """One record as the input partitions supply it."""

    id: str
    lang: str
    title: str
    snippet: str


class StructuredRecord(TypedDict):
    """A bronze HTML partition parsed back into fields."""

    id: str
    lang: str
    title: str
    snippet: str
    normalized_snippet: str
    source_key: str


class GoldRecord(TypedDict):
    """A structured record keyed by language and id."""

    id: str
    lang: str
    title: str
    normalized_snippet: str
    origin_partition: str


SourceRecords = dict[str, SourceRecord]
RenderedPartitions = dict[str, str]
StructuredRecords = dict[str, StructuredRecord]
GoldRecords = dict[str, GoldRecord]


def bronze_render_html(value: SourceRecords, *, context: StepContext) -> RenderedPartitions:
    """Render one source record into a deterministic HTML partition."""
    rendered: RenderedPartitions = {}
    for _key, record in value.items():
        item_id = str(record["id"])
        if item_id in rendered:
            raise ValueError(f"Duplicate record key: {item_id}")
        rendered[item_id] = (
            f"<article data-id='{item_id}' data-lang='{record['lang']}'>"
            f"<h1>{record['title']}</h1>"
            f"<p>{record['snippet']}</p>"
            "</article>"
        )
    return rendered


def silver_extract_structured(
    value: RenderedPartitions, *, context: StepContext
) -> StructuredRecords:
    """Convert every bronze HTML partition back into a structured record."""
    structured: StructuredRecords = {}
    for key, html in sorted(value.items()):
        if key in structured:
            raise ValueError(f"Duplicate record key: {key}")
        snippet = _extract_between(html, "<p>", "</p>")
        structured[key] = StructuredRecord(
            id=_extract_between(html, "data-id='", "'"),
            lang=_extract_between(html, "data-lang='", "'"),
            title=_extract_between(html, "<h1>", "</h1>"),
            snippet=snippet,
            normalized_snippet=snippet.lower(),
            source_key=key,
        )
    return structured


def gold_partition_by_language(value: StructuredRecords, *, context: StepContext) -> GoldRecords:
    """Emit path-style keys (`<lang>/<id>`) to prove nested storage keys.

    These are *storage* partition keys inside one payload. They occupy a
    different namespace from execution partition identity, which the engine
    keeps stable across steps.
    """
    repartitioned: GoldRecords = {}
    for key, record in sorted(value.items()):
        lang = str(record["lang"])
        item_id = str(record["id"])
        output_key = f"{lang}/{item_id}"
        if output_key in repartitioned:
            raise ValueError(f"Duplicate record key: {output_key}")
        repartitioned[output_key] = GoldRecord(
            id=item_id,
            lang=lang,
            title=record["title"],
            normalized_snippet=record["normalized_snippet"],
            origin_partition=key,
        )
    return repartitioned


def build_partitioned_pipeline(input_dir: Path | str | None = None) -> Pipeline:
    """Build the partitioned pipeline declaration."""
    seed_dir = Path(input_dir) if input_dir is not None else _DEFAULT_INPUT_DIR

    return Pipeline(
        name=PIPELINE_NAME,
        source=dataset_as_loader(PartitionedJSONDataset(name="partitioned_seed", path=seed_dir)),
        stages=[
            Stage(
                name=BRONZE_STAGE_NAME,
                entries=[
                    Step(
                        bronze_render_html,
                        output=PartitionedTextDataset(suffix=".html", encoding="utf-8"),
                    )
                ],
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[Step(silver_extract_structured, output=PartitionedJSONDataset())],
            ),
            Stage(
                name=GOLD_STAGE_NAME,
                entries=[Step(gold_partition_by_language, output=PartitionedJSONDataset())],
            ),
        ],
    )


def run_partitioned_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "partitioned-1",
    input_dir: Path | str | None = None,
) -> RunResult:
    """Run the partitioned example and return its structured status."""
    pipeline = build_partitioned_pipeline(input_dir=input_dir)
    return run(durable(pipeline, job_id), output=Path(state_root) / "output")


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
