from __future__ import annotations

import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Any, Mapping

from caravel import JSONDataset, Pipeline, Stage, dataset_as_loader, run, step
from caravel.storage import is_url_path

PIPELINE_NAME = "fsspec_example"
BRONZE_STAGE_NAME = "bronze"
SILVER_STAGE_NAME = "silver"

BRONZE_STEP_NAME = "bronze_collect"
SILVER_TRANSFORM_STEP_NAME = "silver_transform"
SILVER_FINALIZE_STEP_NAME = "silver_finalize"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "input_partitions.json"

load_dotenv()


def _runtime_params(context: object) -> Mapping[str, str]:
    params = getattr(context, "params", {})
    if isinstance(params, dict):
        return {str(key): str(value) for key, value in params.items()}
    return {}


def _as_path_or_url(value: Path | str | None) -> Path | str | None:
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    if is_url_path(value):
        return value
    return Path(value)


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


def _storage_options_for_url(path: str | Path | None) -> Mapping[str, Any] | None:
    rendered = str(path)
    if rendered.startswith("abfs://") or rendered.startswith("az://"):
        return _azure_storage_options_from_env()
    return None


@step(output=JSONDataset())
def bronze_collect(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    _ = context
    transformed: dict[str, dict[str, Any]] = {}
    for key in sorted(partitions):
        record = partitions[key]
        transformed[key] = {
            **record,
            "bronze": True,
            "doubled_value": int(record["value"]) * 2,
        }
    return transformed


@step(output=JSONDataset())
def silver_transform(
    partitions: dict[str, dict[str, Any]], *, context: object
) -> dict[str, dict[str, Any]]:
    _ = context
    normalized: dict[str, dict[str, Any]] = {}
    for key in sorted(partitions):
        record = partitions[key]
        normalized[key] = {
            **record,
            "normalized": float(record["doubled_value"]) / 4.0,
        }
    return normalized


@step(output=JSONDataset())
def silver_finalize(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, Any]:
    params = _runtime_params(context)
    mode = params.get("mode")
    ordered_keys = sorted(partitions)
    records: dict[str, dict[str, Any]] = {key: partitions[key] for key in ordered_keys}
    total_normalized = sum(float(record["normalized"]) for record in records.values())

    payload: dict[str, Any] = {
        "pipeline": PIPELINE_NAME,
        "count": len(records),
        "ids": ordered_keys,
        "total_normalized": total_normalized,
        "records": records,
    }

    if mode is not None:
        payload["mode"] = mode

    return payload


def build_fsspec_pipeline(
    input_path: Path | str | None = None,
    *,
    bronze_stage_root: Path | str | None = None,
    silver_stage_root: Path | str | None = None,
) -> Pipeline:
    env_path = os.getenv("CARAVEL_INPUT_URL")
    seed_path = _as_path_or_url(input_path if input_path is not None else (env_path or _DEFAULT_INPUT_PATH))
    storage_options = _storage_options_for_url(seed_path)

    resolved_bronze_stage_root = _as_path_or_url(
        bronze_stage_root
        if bronze_stage_root is not None
        else os.getenv("CARAVEL_BRONZE_STAGE_ROOT")
    )
    resolved_silver_stage_root = _as_path_or_url(
        silver_stage_root
        if silver_stage_root is not None
        else os.getenv("CARAVEL_SILVER_STAGE_ROOT")
    )

    loader = dataset_as_loader(
        JSONDataset(
            name="fsspec_seed",
            path=seed_path,
            storage_options=storage_options,
        )
    )

    return Pipeline(
        name=PIPELINE_NAME,
        loader=loader,
        stages=[
            Stage(
                name=BRONZE_STAGE_NAME,
                stage_root=resolved_bronze_stage_root,
                entries=[bronze_collect],
            ),
            Stage(
                name=SILVER_STAGE_NAME,
                stage_root=resolved_silver_stage_root,
                entries=[silver_transform, silver_finalize],
            ),
        ],
    )


def run_fsspec_pipeline(
    *,
    run_root: Path | str | None = None,
    input_path: Path | str | None = None,
    bronze_stage_root: Path | str | None = None,
    silver_stage_root: Path | str | None = None,
) -> Path | str:
    pipeline = build_fsspec_pipeline(
        input_path=input_path,
        bronze_stage_root=bronze_stage_root,
        silver_stage_root=silver_stage_root,
    )
    return run(pipeline, run_root=run_root)


__all__ = [
    "PIPELINE_NAME",
    "BRONZE_STAGE_NAME",
    "SILVER_STAGE_NAME",
    "BRONZE_STEP_NAME",
    "SILVER_TRANSFORM_STEP_NAME",
    "SILVER_FINALIZE_STEP_NAME",
    "build_fsspec_pipeline",
    "run_fsspec_pipeline",
]
