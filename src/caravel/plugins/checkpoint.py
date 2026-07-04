"""First-party checkpoint plugin: durable committed-output evidence.

``CheckpointPlugin`` provides the singleton checkpoint capability. It owns
schema-versioned records beneath its explicitly configured ``metadata_root``:

    <metadata_root>/v1/checkpoints/<node_id>.json

No location is derived from the pipeline or run root; the operator must
configure a root outside any output tree that core may replace or clean.
Records prove that a save completed. They contain no credentials, storage
options, payloads, hashes, parameter values, or environment values.

Removing the plugin leaves records inert: bare core ignores them and rejects
checkpoint-dependent selective requests at binding.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from ..datasets import CheckpointInspectableDataset
from ..storage import ensure_parent_dir, join_path, resolve_fs, to_storage_string
from ..types import (
    CheckpointCommitUncertainError,
    CheckpointIntegrityError,
    Dataset,
    UnsupportedCapabilityError,
    UnsupportedCheckpointVersionError,
)
from .api import CheckpointContext

CHECKPOINT_SCHEMA_VERSION = 1
_SCHEMA_DIRNAME = f"v{CHECKPOINT_SCHEMA_VERSION}"
_CHECKPOINTS_DIRNAME = "checkpoints"

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

_SUPPORTED_DATASET_TYPES = frozenset(
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


def _fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


def _utc_created_at() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


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
    if dataset["type"] not in _SUPPORTED_DATASET_TYPES:
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


def _route_step_index(node_id: str) -> int | None:
    """Return the 1-based route step index encoded in a route-step node ID."""
    match = re.match(r"^stage-\d{3}-entry-\d{3}-route-\d{3}-step-(\d{3})$", node_id)
    if match is None:
        return None
    return int(match.group(1))


class CheckpointPlugin:
    """Singleton checkpoint capability backed by an explicit metadata root.

    The core executor owns execution and data mutation; this plugin owns the
    record lifecycle: invalidate-and-confirm before replacement, publish and
    read back after a successful save, and verify declaration and physical
    output before every reuse verdict.
    """

    def __init__(
        self,
        *,
        metadata_root: str | Path,
        storage_options: Mapping[str, Any] | None = None,
        plugin_id: str = "checkpoint",
    ) -> None:
        root_text = to_storage_string(metadata_root).strip() if metadata_root else ""
        if not root_text:
            raise ValueError(
                "CheckpointPlugin requires an explicit metadata_root; no default "
                "is derived from the pipeline or run root."
            )
        self.plugin_id = plugin_id
        self._metadata_root = root_text
        self._storage_options = dict(storage_options) if storage_options is not None else None

    # -- record store ------------------------------------------------------

    def record_path(self, node_id: str) -> str:
        return join_path(
            self._metadata_root, _SCHEMA_DIRNAME, _CHECKPOINTS_DIRNAME, f"{node_id}.json"
        )

    def read_record(self, node_id: str) -> dict[str, Any] | None:
        """Return the committed record for a node, or None when uncommitted.

        A missing record or a malformed schema-version-1 record means no
        checkpoint. An unsupported schema version raises.
        """
        fs, resolved = resolve_fs(self.record_path(node_id), self._storage_options)
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

    def _invalidate_record(self, node_id: str) -> None:
        """Delete a node's checkpoint record and confirm its absence."""
        record_path = self.record_path(node_id)
        fs, resolved = resolve_fs(record_path, self._storage_options)
        if fs.exists(resolved):
            fs.rm(resolved)
        _fire_failpoint("during_record_invalidation")
        if fs.exists(resolved):
            raise CheckpointIntegrityError(
                f"Could not confirm checkpoint record absence for node '{node_id}' "
                f"at '{record_path}'."
            )

    def _publish_record(self, node_id: str, record: Mapping[str, Any]) -> dict[str, Any]:
        """Write a node's checkpoint record last and verify it before success.

        On an exception during the final write or read-back, the record is
        rediscovered: a valid matching record means the output committed, a
        clearly absent or malformed record re-raises the original failure, and
        an unreadable outcome raises CheckpointCommitUncertainError.
        """
        validated = validate_checkpoint_record(dict(record), expected_node_id=node_id)
        record_path = self.record_path(node_id)

        try:
            fs, resolved = resolve_fs(record_path, self._storage_options)
            ensure_parent_dir(fs, resolved)
            with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
                json.dump(validated, handle, ensure_ascii=False, indent=2)
            _fire_failpoint("after_record_write")
            reread = self.read_record(node_id)
        except UnsupportedCheckpointVersionError:
            raise
        except Exception as write_error:
            try:
                rediscovered = self.read_record(node_id)
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

    # -- capability support ------------------------------------------------

    def _supported_dataset(self, context: CheckpointContext, dataset: Dataset) -> bool:
        return (
            isinstance(dataset, CheckpointInspectableDataset)
            and context.node.dataset_type in _SUPPORTED_DATASET_TYPES
        )

    def _record_step_index(self, context: CheckpointContext) -> int:
        node = context.node
        if node.kind == "route-step":
            route_index = _route_step_index(node.node_id)
            if route_index is None:
                raise CheckpointIntegrityError(
                    f"Node '{node.node_id}' is a route step but its node ID does not "
                    "encode a route step index."
                )
            return route_index
        return node.entry_index

    # -- CheckpointCapability ----------------------------------------------

    def reuse_verdict(self, context: CheckpointContext, dataset: Dataset) -> bool:
        """Bless a node's output as reusable only on verified committed evidence."""
        node = context.node
        if not self._supported_dataset(context, dataset):
            raise UnsupportedCapabilityError(
                f"Dataset '{node.dataset_type}' for stage='{node.stage_name}' "
                f"step='{node.name}' does not support checkpoint inspection and "
                "cannot serve as a checkpoint-backed selective boundary."
            )

        record = self.read_record(node.node_id)
        if record is None:
            return False

        dataset_block = record["dataset"]
        if (
            record["pipeline"] != context.run.pipeline_name
            or dataset_block["type"] != node.dataset_type
            or dataset_block["name"] != node.dataset_name
            or record["output_path"] != node.step_dir
        ):
            raise CheckpointIntegrityError(
                "Checkpoint record does not match the current declaration: "
                f"stage='{node.stage_name}' step='{node.name}' node='{node.node_id}' "
                f"path='{node.step_dir}'."
            )

        assert isinstance(dataset, CheckpointInspectableDataset)
        assert node.step_dir is not None
        dataset.verify_physical_output(node.step_dir, record["partition_keys"])
        return True

    def before_replacement(self, context: CheckpointContext, dataset: Dataset) -> None:
        """Invalidate prior evidence and confirm absence before output replacement."""
        if not self._supported_dataset(context, dataset):
            return
        _fire_failpoint("before_record_invalidation")
        self._invalidate_record(context.node.node_id)
        _fire_failpoint("after_record_invalidation")

    def after_save(self, context: CheckpointContext, dataset: Dataset) -> None:
        """Build, publish, and read back new evidence only after a successful save."""
        if not self._supported_dataset(context, dataset):
            return
        node = context.node
        assert isinstance(dataset, CheckpointInspectableDataset)
        assert node.step_dir is not None
        _fire_failpoint("before_record_write")
        partition_keys = dataset.physical_partition_keys(node.step_dir)
        record = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "run_id": context.run.run_id,
            "node_id": node.node_id,
            "pipeline": context.run.pipeline_name,
            "stage": {"index": node.stage_index, "name": node.stage_name},
            "step": {"index": self._record_step_index(context), "name": node.name},
            "dataset": {"type": node.dataset_type, "name": node.dataset_name},
            "output_path": node.step_dir,
            "partition_keys": partition_keys,
            "count": 1 if partition_keys is None else len(partition_keys),
            "created_at": _utc_created_at(),
        }
        self._publish_record(node.node_id, record)
        _fire_failpoint("after_record_verified")


__all__ = [
    "CHECKPOINT_SCHEMA_VERSION",
    "CheckpointPlugin",
    "validate_checkpoint_record",
]
