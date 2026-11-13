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
    Step,
    run,
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


def bronze_normalize_json_source_partition(
    partition, *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize one JSON-sourced record. Runs only for partitions routed here."""
    record: dict[str, Any] = partition.load()
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


def bronze_normalize_json_source(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(bronze_normalize_json_source_partition)


def bronze_normalize_html_source_partition(
    partition, *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize one HTML-sourced record. Runs only for partitions routed here."""
    record: dict[str, Any] = partition.load()
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


def bronze_normalize_html_source(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(bronze_normalize_html_source_partition)


def silver_unify_records_partition(partition, *, context: object) -> dict[str, dict[str, Any]]:
    """Unify one partition's normalized record."""
    payload: dict[str, dict[str, Any]] = partition.load()
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


def silver_unify_records(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(silver_unify_records_partition)


def bronze_converged_records_partition(partition, *, context: object) -> dict[str, dict[str, Any]]:
    """Pass one converged partition through."""
    payload: dict[str, dict[str, Any]] = partition.load()
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


def bronze_converged_records(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(bronze_converged_records_partition)


def gold_partition_by_language_partition(
    partition, *, context: object
) -> dict[str, dict[str, Any]]:
    """Key one partition's record by language."""
    payload: dict[str, dict[str, Any]] = partition.load()
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


def gold_partition_by_language(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(gold_partition_by_language_partition)


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
            JSON_SOURCE_NAME: [Step(bronze_normalize_json_source, output=PartitionedJSONDataset())],
            HTML_SOURCE_NAME: [Step(bronze_normalize_html_source, output=PartitionedJSONDataset())],
        },
    )

    return Pipeline(
        name=PIPELINE_NAME,
        source=loader,
        stages=[
            Stage(
                name=BRONZE_STAGE_NAME,
                entries=[
                    branch_entry,
                    Step(bronze_converged_records, output=PartitionedJSONDataset()),
                ],
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[Step(silver_unify_records, output=PartitionedJSONDataset())],
            ),
            Stage(
                name=GOLD_STAGE_NAME,
                entries=[Step(gold_partition_by_language, output=PartitionedJSONDataset())],
            ),
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
        storage=DurableStorage.at(Path(state_root) / "output"),
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
