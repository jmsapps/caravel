from __future__ import annotations

from pathlib import Path
from typing import Any

from caravel import (
    SOURCE_FIELD,
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
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize JSON-source records onto a shared schema keyed by logical `id`."""
    _ = context
    normalized: dict[str, dict[str, Any]] = {}

    for key in sorted(partitions):
        record = partitions[key]
        if not isinstance(record, dict):
            raise TypeError(
                f"Expected dict record for partition '{key}', got {type(record).__name__}."
            )

        source_name = str(record.get(SOURCE_FIELD, ""))
        if source_name != JSON_SOURCE_NAME:
            raise ValueError(
                f"Expected source '{JSON_SOURCE_NAME}' for partition '{key}', got '{source_name}'."
            )

        item_id = str(record["id"])
        language = str(record["language"])
        content = str(record["body"])

        normalized[item_id] = {
            "id": item_id,
            "language": language,
            "source": source_name,
            "content": content,
            "title": str(record.get("title", "")),
            "origin_partition": key,
        }

    return normalized


@step(output=PartitionedJSONDataset())
def bronze_normalize_html_source(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Normalize HTML-source records onto the same schema keyed by logical `id`."""
    _ = context
    normalized: dict[str, dict[str, Any]] = {}

    for key in sorted(partitions):
        record = partitions[key]
        if not isinstance(record, dict):
            raise TypeError(
                f"Expected dict record for partition '{key}', got {type(record).__name__}."
            )

        source_name = str(record.get(SOURCE_FIELD, ""))
        if source_name != HTML_SOURCE_NAME:
            raise ValueError(
                f"Expected source '{HTML_SOURCE_NAME}' for partition '{key}', got '{source_name}'."
            )

        html = str(record["content"])
        item_id = _extract_between(html, 'data-id="', '"')
        language = _extract_between(html, 'data-lang="', '"')
        title = _extract_between(html, "<h1>", "</h1>")
        content = _extract_between(html, "<p>", "</p>")

        normalized[item_id] = {
            "id": item_id,
            "language": language,
            "source": source_name,
            "content": content,
            "title": title,
            "origin_partition": key,
        }

    return normalized


@step(output=PartitionedJSONDataset())
def silver_unify_records(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Validate and preserve a uniform record contract after branch convergence."""
    _ = context
    unified: dict[str, dict[str, Any]] = {}

    for key in sorted(partitions):
        record = partitions[key]
        required = {"id", "language", "source", "content"}
        missing = required.difference(record)
        if missing:
            raise KeyError(f"Unified record '{key}' missing required fields: {sorted(missing)}")

        unified[key] = {
            "id": str(record["id"]),
            "language": str(record["language"]),
            "source": str(record["source"]),
            "content": str(record["content"]),
            "title": str(record.get("title", "")),
            "origin_partition": str(record.get("origin_partition", key)),
        }

    return unified


@step(output=PartitionedJSONDataset())
def bronze_converged_records(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Persist merged branch output as a stage-terminal Step for downstream seeding."""
    _ = context
    return {key: dict(record) for key, record in sorted(partitions.items())}


@step(output=PartitionedJSONDataset())
def gold_partition_by_language(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    """Repartition converged records using path-style language keys (`<lang>/<id>`)."""
    _ = context
    repartitioned: dict[str, dict[str, Any]] = {}

    for key in sorted(partitions):
        record = partitions[key]
        item_id = str(record["id"])
        language = str(record["language"])

        repartitioned[f"{language}/{item_id}"] = {
            "id": item_id,
            "language": language,
            "source": str(record["source"]),
            "content": str(record["content"]),
            "origin_key": key,
        }

    return repartitioned


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
        loader=loader,
        stages=[
            Stage(name=BRONZE_STAGE_NAME, entries=[branch_entry, bronze_converged_records]),
            Stage(name=SILVER_STAGE_NAME, entries=[silver_unify_records]),
            Stage(name=GOLD_STAGE_NAME, entries=[gold_partition_by_language]),
        ],
    )


def run_branching_pipeline(
    *,
    run_root: Path | str | None = None,
    json_source_dir: Path | str | None = None,
    html_source_dir: Path | str | None = None,
) -> Path | str:
    """Run the branching pipeline and return the resolved run root."""
    pipeline = build_branching_pipeline(
        json_source_dir=json_source_dir,
        html_source_dir=html_source_dir,
    )
    return run(pipeline, run_root=run_root)


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
