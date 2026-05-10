from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from dotenv import load_dotenv

from caravel import (
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    Step,
    dataset_as_loader,
    run,
)
from caravel.storage import is_url_path

PIPELINE_NAME = "fsspec_partitioned_example"

BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"
GOLD_STAGE_NAME = "gold"

BRONZE_STEP_NAME = "bronze_render_html"
SILVER_STEP_NAME = "silver_extract_structured"
GOLD_STEP_NAME = "gold_partition_by_language"

_DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "data" / "input_partitions"

load_dotenv()


def _extract_between(value: str, start: str, end: str) -> str:
    start_idx = value.index(start) + len(start)
    end_idx = value.index(end, start_idx)
    return value[start_idx:end_idx]


def _as_path_or_url(value: Path | str | None) -> Path | str | None:
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    if is_url_path(value):
        return value
    return Path(value)


def _parse_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value '{value}'.")


def _azure_storage_options_from_env() -> dict[str, str] | None:
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    options: dict[str, str] = {}
    if account_name:
        options["account_name"] = account_name

    if account_key:
        options["account_key"] = account_key
    elif sas_token:
        options["sas_token"] = sas_token
    elif tenant_id and client_id and client_secret:
        options["tenant_id"] = tenant_id
        options["client_id"] = client_id
        options["client_secret"] = client_secret
    elif client_id:
        # User-assigned managed identity may require an explicit client id.
        options["client_id"] = client_id

    return options or None


def _storage_options_for_path(path: Path | str | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    rendered = str(path)
    if rendered.startswith("abfs://") or rendered.startswith("az://"):
        return _azure_storage_options_from_env()
    return None


def bronze_render_html(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, str]:
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


def silver_extract_structured(
    partitions: dict[str, str], *, context: object
) -> dict[str, dict[str, Any]]:
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


def gold_partition_by_language(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
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


def build_fsspec_partitioned_pipeline(
    *,
    input_dir: Path | str | None = None,
    run_root: Path | str | None = None,
    bronze_stage_root: Path | str | None = None,
    silver_stage_root: Path | str | None = None,
    gold_stage_root: Path | str | None = None,
    bronze_clean_dirs: bool | None = None,
    silver_clean_dirs: bool | None = None,
    gold_clean_dirs: bool | None = None,
) -> Pipeline:
    env_input_dir = os.getenv("CARAVEL_INPUT_DIR_URL")
    env_run_root = os.getenv("CARAVEL_RUN_ROOT")

    resolved_input_dir = _as_path_or_url(input_dir if input_dir is not None else (env_input_dir or _DEFAULT_INPUT_DIR))
    resolved_run_root = _as_path_or_url(run_root if run_root is not None else env_run_root)

    resolved_bronze_stage_root = _as_path_or_url(
        bronze_stage_root if bronze_stage_root is not None else "abfs://docs-bronze-container"
    )
    resolved_silver_stage_root = _as_path_or_url(
        silver_stage_root if silver_stage_root is not None else "abfs://docs-silver-container"
    )
    resolved_gold_stage_root = _as_path_or_url(
        gold_stage_root if gold_stage_root is not None else "abfs://docs-gold-container"
    )

    resolved_bronze_clean = (
        bronze_clean_dirs
        if bronze_clean_dirs is not None
        else _parse_bool(os.getenv("CARAVEL_BRONZE_CLEAN_DIRS"), default=True)
    )
    resolved_silver_clean = (
        silver_clean_dirs
        if silver_clean_dirs is not None
        else _parse_bool(os.getenv("CARAVEL_SILVER_CLEAN_DIRS"), default=True)
    )
    resolved_gold_clean = (
        gold_clean_dirs
        if gold_clean_dirs is not None
        else _parse_bool(os.getenv("CARAVEL_GOLD_CLEAN_DIRS"), default=True)
    )

    loader_storage_options = _storage_options_for_path(resolved_input_dir)

    bronze_output_options = _storage_options_for_path(
        resolved_bronze_stage_root if resolved_bronze_stage_root is not None else resolved_run_root
    )
    silver_output_options = _storage_options_for_path(
        resolved_silver_stage_root if resolved_silver_stage_root is not None else resolved_run_root
    )
    gold_output_options = _storage_options_for_path(
        resolved_gold_stage_root if resolved_gold_stage_root is not None else resolved_run_root
    )

    loader = dataset_as_loader(
        PartitionedJSONDataset(
            name="fsspec_partitioned_seed",
            path=resolved_input_dir,
            storage_options=loader_storage_options,
        )
    )

    bronze_step = Step(
        fn=bronze_render_html,
        name=BRONZE_STEP_NAME,
        output=PartitionedTextDataset(
            name=BRONZE_STEP_NAME,
            suffix=".html",
            encoding="utf-8",
            storage_options=bronze_output_options,
        ),
    )
    silver_step = Step(
        fn=silver_extract_structured,
        name=SILVER_STEP_NAME,
        output=PartitionedJSONDataset(
            name=SILVER_STEP_NAME,
            storage_options=silver_output_options,
        ),
    )
    gold_step = Step(
        fn=gold_partition_by_language,
        name=GOLD_STEP_NAME,
        output=PartitionedJSONDataset(
            name=GOLD_STEP_NAME,
            storage_options=gold_output_options,
        ),
    )

    return Pipeline(
        name=PIPELINE_NAME,
        loader=loader,
        stages=[
            Stage(
                name=BRONZE_STAGE_NAME,
                entries=[bronze_step],
                stage_root=resolved_bronze_stage_root,
                clean_dirs=resolved_bronze_clean,
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                entries=[silver_step],
                stage_root=resolved_silver_stage_root,
                clean_dirs=resolved_silver_clean,
            ),
            Stage(
                name=GOLD_STAGE_NAME,
                entries=[gold_step],
                stage_root=resolved_gold_stage_root,
                clean_dirs=resolved_gold_clean,
            ),
        ],
    )


def run_fsspec_partitioned_pipeline(
    *,
    run_root: Path | str | None = None,
    input_dir: Path | str | None = None,
    bronze_stage_root: Path | str | None = None,
    silver_stage_root: Path | str | None = None,
    gold_stage_root: Path | str | None = None,
    bronze_clean_dirs: bool | None = None,
    silver_clean_dirs: bool | None = None,
    gold_clean_dirs: bool | None = None,
) -> Path | str:
    resolved_run_root = _as_path_or_url(run_root if run_root is not None else os.getenv("CARAVEL_RUN_ROOT"))
    pipeline = build_fsspec_partitioned_pipeline(
        input_dir=input_dir,
        run_root=resolved_run_root,
        bronze_stage_root=bronze_stage_root,
        silver_stage_root=silver_stage_root,
        gold_stage_root=gold_stage_root,
        bronze_clean_dirs=bronze_clean_dirs,
        silver_clean_dirs=silver_clean_dirs,
        gold_clean_dirs=gold_clean_dirs,
    )
    return run(pipeline, run_root=resolved_run_root)


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
