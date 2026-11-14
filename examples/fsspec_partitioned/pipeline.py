"""Partitioned output with remote-capable artifact storage.

Same storage envelope as ``fsspec_minimal``: the authoritative progress store
is local, and committed artifacts may live in object storage.

The retired runner let each stage pick its own output root (``stage_root``).
Durable execution owns artifact placement, because committed output must be
reachable from the manifest; one ``artifacts_root`` replaces the per-stage
roots.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, TypedDict

from caravel import (
    StepContext,
    RunResult,
    JSONDataset,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
)

from ..support import durable

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


class LanguageSummary(TypedDict):
    """Record counts per language across the whole run."""

    pipeline: str
    total_records: int
    counts_by_language: dict[str, int]


SourceRecords = dict[str, SourceRecord]
RenderedPartitions = dict[str, str]
StructuredRecords = dict[str, StructuredRecord]


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
        )
    return structured


def gold_summarize(value: StructuredRecords, *, context: StepContext) -> LanguageSummary:
    """Summarize every structured record by language."""
    del context
    languages: dict[str, int] = {}
    for record in value.values():
        languages[record["lang"]] = languages.get(record["lang"], 0) + 1
    return LanguageSummary(
        pipeline=PIPELINE_NAME,
        total_records=len(value),
        counts_by_language=dict(sorted(languages.items())),
    )


def build_fsspec_partitioned_pipeline(
    input_dir: Path | str | None = None,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> Pipeline:
    seed = input_dir if input_dir is not None else _DEFAULT_INPUT_DIR
    return Pipeline(
        name=PIPELINE_NAME,
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
                entries=[
                    Step(
                        gold_summarize,
                        output=JSONDataset(name="summary"),
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
) -> RunResult:
    """Run the example and return its structured status."""
    pipeline = build_fsspec_partitioned_pipeline(
        input_dir=input_dir, storage_options=storage_options
    )
    return run(durable(pipeline, job_id), output=Path(state_root) / "output")


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
