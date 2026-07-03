from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any, Callable, Mapping

import fsspec

from .types import (
    CheckpointCommitUncertainError,
    CheckpointIntegrityError,
    UnsupportedCheckpointVersionError,
)

StoragePath = str | Path

METADATA_DIRNAME = "_000_metadata"
CHECKPOINTS_DIRNAME = "checkpoints"
CHECKPOINT_SCHEMA_VERSION = 1

_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_NODE_ID_RE = re.compile(r"^stage-\d{3}-entry-\d{3}(?:-route-\d{3}-step-\d{3})?$")
_CREATED_AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")

_CHECKPOINT_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "node_id",
        "pipeline",
        "stage",
        "step",
        "dataset",
        "output_path",
        "partition_keys",
        "count",
        "created_at",
    }
)

_BUILTIN_DATASET_TYPES = frozenset(
    {
        "JSONDataset",
        "PartitionedJSONDataset",
        "TextDataset",
        "PartitionedTextDataset",
        "BytesDataset",
        "PartitionedBytesDataset",
    }
)

# Deterministic failure injection for checkpoint contract tests. Production
# code leaves the hook unset; tests assign a callable that raises at a chosen
# failpoint name.
_failpoint_hook: Callable[[str], None] | None = None


def fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


def is_url_path(path: StoragePath) -> bool:
    return "://" in str(path)


def coerce_optional_storage_path(path: str | Path | None) -> StoragePath | None:
    if path is None:
        return None
    if isinstance(path, Path):
        return path
    if is_url_path(path):
        return path
    return Path(path)


def ensure_storage_path_set(name: str, path: StoragePath | None) -> StoragePath:
    if path is None:
        raise FileNotFoundError(f"Dataset '{name}' path is not set (path=None).")
    return path


def to_storage_string(path: StoragePath) -> str:
    if isinstance(path, Path):
        return path.as_posix()
    return path


def resolve_fs(
    path: StoragePath, storage_options: Mapping[str, Any] | None = None
) -> tuple[Any, str]:
    options = dict(storage_options or {})
    fs, resolved_path = fsspec.core.url_to_fs(to_storage_string(path), **options)
    return fs, str(resolved_path)


def join_path(base: StoragePath, *parts: str) -> str:
    assembled = to_storage_string(base)
    for part in parts:
        clean = part.strip("/")
        if clean:
            assembled = posixpath.join(assembled, clean)
    return assembled


def parent_path(path: StoragePath) -> str:
    normalized = to_storage_string(path).rstrip("/")
    return posixpath.dirname(normalized)


def leaf_name(path: StoragePath) -> str:
    normalized = to_storage_string(path).rstrip("/")
    return posixpath.basename(normalized)


def single_output_path(dest: StoragePath, suffix: str) -> str:
    leaf = leaf_name(dest)
    return join_path(dest, f"{leaf}{suffix}")


def ensure_parent_dir(fs: Any, path: StoragePath) -> None:
    parent = parent_path(path)
    if parent:
        fs.makedirs(parent, exist_ok=True)


def is_file(fs: Any, path: str) -> bool:
    isfile = getattr(fs, "isfile", None)
    if callable(isfile):
        try:
            return bool(isfile(path))
        except Exception:
            pass

    info = fs.info(path)
    return str(info.get("type", "")) == "file"


def iter_files_with_suffix(
    root: StoragePath,
    suffix: str,
    storage_options: Mapping[str, Any] | None = None,
) -> list[str]:
    fs, root_path = resolve_fs(root, storage_options)
    if not fs.exists(root_path):
        return []

    if is_file(fs, root_path):
        return [root_path] if root_path.endswith(suffix) else []

    try:
        candidates = list(fs.find(root_path, withdirs=False, detail=False))
    except Exception:
        pattern = join_path(root_path, "**")
        candidates = list(fs.glob(pattern))

    file_paths: list[str] = []
    for candidate in candidates:
        path = str(candidate)
        if path.endswith(suffix) and is_file(fs, path):
            file_paths.append(path)

    return sorted(file_paths)


def prepare_partitioned_save(fs: Any, destination: str) -> None:
    """Create a partition destination directory."""
    fs.makedirs(destination, exist_ok=True)


def partitioned_output_exists(
    root: StoragePath,
    suffix: str,
    storage_options: Mapping[str, Any] | None = None,
) -> bool:
    """Return whether at least one physical partition file exists."""
    return bool(iter_files_with_suffix(root, suffix, storage_options))


def remove_and_recreate_dir(
    path: StoragePath, storage_options: Mapping[str, Any] | None = None
) -> None:
    """Replace a Caravel-managed output directory with an empty one."""
    fs, resolved = resolve_fs(path, storage_options)
    if fs.exists(resolved):
        fs.rm(resolved, recursive=True)
    fs.makedirs(resolved, exist_ok=True)


def checkpoint_record_path(pipeline_root: StoragePath, node_id: str) -> str:
    return join_path(pipeline_root, METADATA_DIRNAME, CHECKPOINTS_DIRNAME, f"{node_id}.json")


def _validate_index_name_block(record: Mapping[str, Any], field: str) -> None:
    block = record[field]
    if not isinstance(block, dict) or set(block) != {"index", "name"}:
        raise ValueError(f"Checkpoint record field '{field}' must contain exactly index and name.")
    index = block["index"]
    if not isinstance(index, int) or isinstance(index, bool) or index < 1:
        raise ValueError(f"Checkpoint record '{field}.index' must be a positive integer.")
    name = block["name"]
    if not isinstance(name, str) or not name:
        raise ValueError(f"Checkpoint record '{field}.name' must be a non-empty string.")


def validate_checkpoint_record(payload: object, *, expected_node_id: str) -> dict[str, Any]:
    """Strictly validate a schema-version-1 checkpoint record.

    Raises UnsupportedCheckpointVersionError for a non-1 integer schema version
    and ValueError for anything malformed under schema version 1.
    """
    if not isinstance(payload, dict):
        raise ValueError("Checkpoint record must be a JSON object.")

    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ValueError("Checkpoint record schema_version must be an integer.")
    if schema_version != CHECKPOINT_SCHEMA_VERSION:
        raise UnsupportedCheckpointVersionError(
            f"Unsupported checkpoint schema version {schema_version}; "
            f"supported version is {CHECKPOINT_SCHEMA_VERSION}."
        )

    if set(payload) != _CHECKPOINT_RECORD_FIELDS:
        unknown = sorted(set(payload) - _CHECKPOINT_RECORD_FIELDS)
        missing = sorted(_CHECKPOINT_RECORD_FIELDS - set(payload))
        raise ValueError(f"Checkpoint record fields invalid; unknown={unknown} missing={missing}.")

    run_id = payload["run_id"]
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        raise ValueError("Checkpoint record run_id must be lowercase UUID4 hex.")

    node_id = payload["node_id"]
    if not isinstance(node_id, str) or not _NODE_ID_RE.match(node_id):
        raise ValueError("Checkpoint record node_id has an invalid shape.")
    if node_id != expected_node_id:
        raise ValueError(
            f"Checkpoint record node_id '{node_id}' does not match expected '{expected_node_id}'."
        )

    pipeline = payload["pipeline"]
    if not isinstance(pipeline, str) or not pipeline:
        raise ValueError("Checkpoint record pipeline must be a non-empty string.")

    _validate_index_name_block(payload, "stage")
    _validate_index_name_block(payload, "step")

    dataset = payload["dataset"]
    if not isinstance(dataset, dict) or set(dataset) != {"type", "name"}:
        raise ValueError("Checkpoint record dataset must contain exactly type and name.")
    if dataset["type"] not in _BUILTIN_DATASET_TYPES:
        raise ValueError(f"Checkpoint record dataset type '{dataset['type']}' is not supported.")
    if not isinstance(dataset["name"], str) or not dataset["name"]:
        raise ValueError("Checkpoint record dataset name must be a non-empty string.")

    output_path = payload["output_path"]
    if not isinstance(output_path, str) or not output_path:
        raise ValueError("Checkpoint record output_path must be a non-empty string.")

    partition_keys = payload["partition_keys"]
    count = payload["count"]
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise ValueError("Checkpoint record count must be a non-negative integer.")
    if partition_keys is None:
        if count != 1:
            raise ValueError("Checkpoint record count must be 1 for a single dataset.")
    elif isinstance(partition_keys, list):
        if not all(isinstance(key, str) and key for key in partition_keys):
            raise ValueError("Checkpoint record partition_keys must be non-empty strings.")
        if partition_keys != sorted(set(partition_keys)):
            raise ValueError("Checkpoint record partition_keys must be sorted and unique.")
        if count != len(partition_keys):
            raise ValueError("Checkpoint record count must equal the number of partition keys.")
    else:
        raise ValueError("Checkpoint record partition_keys must be null or a list of strings.")

    created_at = payload["created_at"]
    if not isinstance(created_at, str) or not _CREATED_AT_RE.match(created_at):
        raise ValueError("Checkpoint record created_at must be UTC RFC 3339 with a trailing Z.")

    return dict(payload)


def read_checkpoint_record(
    pipeline_root: StoragePath,
    node_id: str,
    storage_options: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return the committed record for a node, or None when uncommitted.

    A missing record or a malformed schema-version-1 record means no
    checkpoint. An unsupported schema version raises.
    """
    record_path = checkpoint_record_path(pipeline_root, node_id)
    fs, resolved = resolve_fs(record_path, storage_options)
    if not fs.exists(resolved):
        return None

    try:
        with fs.open(resolved, mode="rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return None

    try:
        return validate_checkpoint_record(payload, expected_node_id=node_id)
    except UnsupportedCheckpointVersionError:
        raise
    except ValueError:
        return None


def invalidate_checkpoint_record(
    pipeline_root: StoragePath,
    node_id: str,
    storage_options: Mapping[str, Any] | None = None,
) -> None:
    """Delete a node's checkpoint record and confirm its absence."""
    record_path = checkpoint_record_path(pipeline_root, node_id)
    fs, resolved = resolve_fs(record_path, storage_options)
    if fs.exists(resolved):
        fs.rm(resolved)
    if fs.exists(resolved):
        raise CheckpointIntegrityError(
            f"Could not confirm checkpoint record absence for node '{node_id}' at '{record_path}'."
        )


def publish_checkpoint_record(
    pipeline_root: StoragePath,
    node_id: str,
    record: Mapping[str, Any],
    storage_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Write a node's checkpoint record last and verify it before success.

    On an exception during the final write or read-back, the record is
    rediscovered: a valid matching record means the output committed, a
    clearly absent or malformed record re-raises the original failure, and an
    unreadable outcome raises CheckpointCommitUncertainError.
    """
    validated = validate_checkpoint_record(dict(record), expected_node_id=node_id)
    record_path = checkpoint_record_path(pipeline_root, node_id)

    try:
        fs, resolved = resolve_fs(record_path, storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
            json.dump(validated, handle, ensure_ascii=False, indent=2)
        fire_failpoint("after_record_write")
        reread = read_checkpoint_record(pipeline_root, node_id, storage_options)
    except UnsupportedCheckpointVersionError:
        raise
    except Exception as write_error:
        try:
            rediscovered = read_checkpoint_record(pipeline_root, node_id, storage_options)
        except Exception as rediscover_error:
            raise CheckpointCommitUncertainError(
                f"Checkpoint publication outcome for node '{node_id}' at "
                f"'{record_path}' could not be determined."
            ) from rediscover_error
        if rediscovered == validated:
            return validated
        raise write_error

    if reread != validated:
        raise CheckpointCommitUncertainError(
            f"Checkpoint record read-back for node '{node_id}' at '{record_path}' "
            "did not match the published record."
        )
    return validated


def relative_key_from_file(root: StoragePath, file_path: StoragePath, suffix: str) -> str:
    root_text = to_storage_string(root).rstrip("/")
    file_text = to_storage_string(file_path)

    prefix = f"{root_text}/"
    if file_text.startswith(prefix):
        rel = file_text[len(prefix) :]
    elif file_text == root_text:
        rel = ""
    else:
        rel = file_text

    if suffix and rel.endswith(suffix):
        return rel[: -len(suffix)]
    return rel


def is_dir(fs: Any, path: str) -> bool:
    isdir = getattr(fs, "isdir", None)
    if callable(isdir):
        try:
            return bool(isdir(path))
        except Exception:
            pass

    info = fs.info(path)
    return str(info.get("type", "")) == "directory"
