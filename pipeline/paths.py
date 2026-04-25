"""Path and naming helpers for the PoC pipeline framework."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def format_stage_dir(index: int, name: str) -> str:
    """Return canonical stage folder name like ``_001_bronze``."""
    return f"_{index:03d}_{name}"


def format_step_dir(index: int, name: str) -> str:
    """Return canonical step folder name like ``_002_parse``."""
    return f"_{index:03d}_{name}"


def resolve_step_output_dir(
    run_root: Path,
    stage_idx: int,
    stage_name: str,
    step_idx: int,
    step_name: str,
) -> Path:
    """Return the absolute output directory for a specific stage/step."""
    return (
        Path(run_root)
        / format_stage_dir(stage_idx, stage_name)
        / format_step_dir(step_idx, step_name)
    )


def _utc_now() -> datetime:
    """Return current UTC datetime.

    Kept as a helper to make tests deterministic via monkeypatch.
    """
    return datetime.now(timezone.utc)


def resolve_run_root(pipeline_name: str, override: Path | None = None) -> Path:
    """Resolve run root path.

    If ``override`` is provided, it is returned as-is.
    Otherwise defaults to:
    ``<repo_root>/src/poc/data/<pipeline_name>/<UTC_timestamp>/``
    where timestamp format is ``YYYY-MM-DDTHHMMSSZ``.
    """
    if override is not None:
        return Path(override)

    timestamp = _utc_now().strftime("%Y-%m-%dT%H%M%SZ")
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "src" / "poc" / "data" / pipeline_name / timestamp


def validate_partition_key(key: str) -> None:
    """Validate a partition key for safe nested-path conversion.

    Rules:
    - Forbid backslash (`\\`).
    - Forbid leading slash (`/`).
    - Forbid traversal segment (`..`).
    - Allow internal slash (`/`) for nested paths.
    """
    if not key:
        raise ValueError("Partition key must be non-empty.")

    if "\\" in key:
        raise ValueError(f"Invalid partition key '{key}': backslash is not allowed.")

    if key.startswith("/"):
        raise ValueError(f"Invalid partition key '{key}': leading '/' is not allowed.")

    parts = key.split("/")
    if any(part == ".." for part in parts):
        raise ValueError(f"Invalid partition key '{key}': '..' segment is not allowed.")


def partition_key_to_relpath(key: str, suffix: str) -> Path:
    """Convert a validated partition key into a relative filesystem path.

    Only the leaf gets the suffix.
    Example: ``en/record_001`` + ``.json`` -> ``en/record_001.json``
    """
    validate_partition_key(key)
    parts = key.split("/")
    parent_parts = parts[:-1]
    leaf = parts[-1]
    return Path(*parent_parts, f"{leaf}{suffix}")
