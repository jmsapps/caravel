from __future__ import annotations

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


def resolve_run_root(override: Path | str | None = None) -> Path:
    """Resolve run root path, defaulting to ``data/output`` when omitted."""
    if override is None:
        return Path("data/output")
    return Path(override)


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
