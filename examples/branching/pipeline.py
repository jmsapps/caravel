from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
    DurableStorage,
    Branch,
    MultiSourceLoader,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    run,
    step,
)

PIPELINE_NAME = "branching_example"

BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"
GOLD_STAGE_NAME = "gold"

BRANCH_STEP_NAME = "branch_by_source"
BRONZE_ROUTE_JSON_STEP_NAME = "bronze_normalize_json_source"
BRONZE_ROUTE_HTML_STEP_NAME = "bronze_normalize_html_source"
SILVER_STEP_NAME = "silver_unify_records"
GOLD_STEP_NAME = "gold_partition_by_language"

JSON_SOURCE_NAME = "json_source"
HTML_SOURCE_NAME = "html_source"

_DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"
_DEFAULT_JSON_SOURCE_DIR = _DEFAULT_DATA_DIR / JSON_SOURCE_NAME
_DEFAULT_HTML_SOURCE_DIR = _DEFAULT_DATA_DIR / HTML_SOURCE_NAME

_DEFAULT_COLLISION_DIR = Path(__file__).resolve().parent / "data_collision"
_DEFAULT_COLLISION_JSON_SOURCE_DIR = _DEFAULT_COLLISION_DIR / JSON_SOURCE_NAME
_DEFAULT_COLLISION_HTML_SOURCE_DIR = _DEFAULT_COLLISION_DIR / HTML_SOURCE_NAME


def _extract_between(value: str, start: str, end: str) -> str:
    """Extract deterministic substring boundaries from fixture HTML."""
    start_idx = value.index(start) + len(start)
    end_idx = value.index(end, start_idx)
    return value[start_idx:end_idx]


def make_branching_loader(
    *,
    json_source_dir: Path | str | None = None,
    html_source_dir: Path | str | None = None,
) -> MultiSourceLoader:
    """Build multisource loader over JSON and HTML fixture directories."""
    resolved_json = (
        Path(json_source_dir) if json_source_dir is not None else _DEFAULT_JSON_SOURCE_DIR
    )
    resolved_html = (
        Path(html_source_dir) if html_source_dir is not None else _DEFAULT_HTML_SOURCE_DIR
    )

    return MultiSourceLoader(
        [
            PartitionedJSONDataset(name=JSON_SOURCE_NAME, path=resolved_json),
            PartitionedTextDataset(name=HTML_SOURCE_NAME, path=resolved_html, suffix=".html"),
        ]
    )


@step(output=PartitionedJSONDataset())
def bronze_normalize_json_source(
    record: dict[str, Any], *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize one JSON-sourced record. Runs only for partitions routed here."""
    _ = context
    payload = record.get("content", record)
    item_id = str(payload["id"])
    return {
        item_id: {
            "id": item_id,
            "lang": str(payload["language"]),
            "title": str(payload["title"]),
            "body": str(payload["body"]),
            "origin": "json",
        }
    }


@step(output=PartitionedJSONDataset())
def bronze_normalize_html_source(
    record: dict[str, Any], *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize one HTML-sourced record. Runs only for partitions routed here."""
    _ = context
    html = record.get("content", record)
    if not isinstance(html, str):
        raise TypeError(f"Expected HTML text, got {type(html).__name__}.")
    item_id = _extract_between(html, 'data-id="', '"')
    return {
        item_id: {
            "id": item_id,
            "lang": _extract_between(html, 'data-lang="', '"'),
            "title": _extract_between(html, "<h1>", "</h1>"),
            "body": _extract_between(html, "<p>", "</p>"),
            "origin": "html",
        }
    }


@step(output=PartitionedJSONDataset())
def silver_unify_records(
    payload: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Unify one partition's normalized record."""
    _ = context
    unified: dict[str, dict[str, Any]] = {}
    for key in sorted(payload):
        record = payload[key]
        unified[
            f"{record['lang']}/{record['id']}"
            if "silver_unify_records" == "gold_partition_by_language"
            else key
        ] = {**record, "unified": True}
    return unified


@step(output=PartitionedJSONDataset())
def bronze_converged_records(
    payload: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Pass one converged partition through."""
    _ = context
    unified: dict[str, dict[str, Any]] = {}
    for key in sorted(payload):
        record = payload[key]
        unified[
            f"{record['lang']}/{record['id']}"
            if "bronze_converged_records" == "gold_partition_by_language"
            else key
        ] = {**record, "unified": True}
    return unified


@step(output=PartitionedJSONDataset())
def gold_partition_by_language(
    payload: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Key one partition's record by language."""
    _ = context
    unified: dict[str, dict[str, Any]] = {}
    for key in sorted(payload):
        record = payload[key]
        unified[
            f"{record['lang']}/{record['id']}"
            if "gold_partition_by_language" == "gold_partition_by_language"
            else key
        ] = {**record, "unified": True}
    return unified


def build_branching_pipeline(
    *,
    json_source_dir: Path | str | None = None,
    html_source_dir: Path | str | None = None,
) -> Pipeline:
    """Build the synthetic branching pipeline declaration."""
    loader = make_branching_loader(
        json_source_dir=json_source_dir,
        html_source_dir=html_source_dir,
    )

    branch_entry = Branch(
        name=BRANCH_STEP_NAME,
        by="source",
        routes={
            JSON_SOURCE_NAME: [bronze_normalize_json_source],
            HTML_SOURCE_NAME: [bronze_normalize_html_source],
        },
    )

    return Pipeline(
        name=PIPELINE_NAME,
        source=loader,
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[branch_entry, bronze_converged_records]),
            Stage(name=SILVER_STAGE_NAME, entries=[silver_unify_records]),
            Stage(name=GOLD_STAGE_NAME, entries=[gold_partition_by_language]),
        ],
    )


def run_branching_pipeline(
    *,
    state_root: Path | str,
    job_id: str = "branching-1",
    json_source_dir: Path | str | None = None,
    html_source_dir: Path | str | None = None,
) -> Any:
    """Run the branching example and return its structured status."""
    pipeline = build_branching_pipeline(
        json_source_dir=json_source_dir, html_source_dir=html_source_dir
    )
    return run(
        pipeline,
        job_id=job_id,
        source=pipeline.source,
        storage=DurableStorage.at(state_root),
    )


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "GOLD_STAGE_NAME",
    "BRANCH_STEP_NAME",
    "BRONZE_ROUTE_JSON_STEP_NAME",
    "BRONZE_ROUTE_HTML_STEP_NAME",
    "SILVER_STEP_NAME",
    "GOLD_STEP_NAME",
    "JSON_SOURCE_NAME",
    "HTML_SOURCE_NAME",
    "_DEFAULT_JSON_SOURCE_DIR",
    "_DEFAULT_HTML_SOURCE_DIR",
    "_DEFAULT_COLLISION_JSON_SOURCE_DIR",
    "_DEFAULT_COLLISION_HTML_SOURCE_DIR",
    "make_branching_loader",
    "build_branching_pipeline",
    "run_branching_pipeline",
]
