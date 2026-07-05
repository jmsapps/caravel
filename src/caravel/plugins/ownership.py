"""First-party ownership plugin: durable-inventory cross-run pruning.

``OwnershipPlugin`` provides the singleton ownership capability. It records a
schema-versioned inventory of the managed persisted output directories that
each full bound plan declares:

    <metadata_root>/v1/inventories/<pipeline>.json

On a later full run it deletes exactly the directories that a prior valid
inventory recorded but the current plan no longer declares. Deletion
candidates never come from directory names: ``_NNN_*`` naming is not proof of
ownership, foreign files survive, and ``stage_root`` trees are user-owned and
never pruned. Selective runs neither prune nor replace the inventory, so a
partial bound plan can never become the deletion basis for a later full run.

Removing the plugin disables historical pruning; it does not weaken current
Dataset replacement. In the reference production profile, destructive
cross-run pruning is composed together with ``CheckpointPlugin`` so pruned
paths can never be blessed for reuse; this plugin never creates, updates, or
blesses checkpoint evidence.
"""

from __future__ import annotations

import json
import posixpath
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from ..storage import ensure_parent_dir, join_path, resolve_fs, to_storage_string
from .api import OwnershipContext

INVENTORY_SCHEMA_VERSION = 1
_SCHEMA_DIRNAME = f"v{INVENTORY_SCHEMA_VERSION}"
_INVENTORIES_DIRNAME = "inventories"

_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_CREATED_AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")

_INVENTORY_FIELDS = frozenset(
    {
        "schema_version",
        "pipeline",
        "run_id",
        "managed_root",
        "output_paths",
        "created_at",
    }
)

# Deterministic failure injection for ownership contract tests. Production
# code leaves the hook unset; tests assign a callable that raises at a chosen
# failpoint name.
_failpoint_hook: Callable[[str], None] | None = None


def _fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


def _utc_created_at() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class OwnershipIntegrityError(Exception):
    """Raised when recorded ownership evidence cannot authorize safe deletion."""


class UnsupportedInventoryVersionError(Exception):
    """Raised when an inventory was written by an unsupported schema version."""


def validate_inventory(payload: object, *, expected_pipeline: str) -> dict[str, Any]:
    """Strictly validate a schema-version-1 ownership inventory.

    Raises UnsupportedInventoryVersionError for a non-1 integer schema version
    and ValueError for anything malformed under schema version 1.
    """
    if not isinstance(payload, dict):
        raise ValueError("Ownership inventory must be a JSON object.")

    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ValueError("Ownership inventory schema_version must be an integer.")
    if schema_version != INVENTORY_SCHEMA_VERSION:
        raise UnsupportedInventoryVersionError(
            f"Unsupported ownership inventory schema version {schema_version}; "
            f"supported version is {INVENTORY_SCHEMA_VERSION}."
        )

    if set(payload) != _INVENTORY_FIELDS:
        unknown = sorted(set(payload) - _INVENTORY_FIELDS)
        missing = sorted(_INVENTORY_FIELDS - set(payload))
        raise ValueError(
            f"Ownership inventory fields invalid; unknown={unknown} missing={missing}."
        )

    pipeline = payload["pipeline"]
    if not isinstance(pipeline, str) or not pipeline:
        raise ValueError("Ownership inventory pipeline must be a non-empty string.")
    if pipeline != expected_pipeline:
        raise ValueError(
            f"Ownership inventory pipeline '{pipeline}' does not match "
            f"expected '{expected_pipeline}'."
        )

    run_id = payload["run_id"]
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        raise ValueError("Ownership inventory run_id must be lowercase UUID4 hex.")

    managed_root = payload["managed_root"]
    if not isinstance(managed_root, str) or not managed_root:
        raise ValueError("Ownership inventory managed_root must be a non-empty string.")

    output_paths = payload["output_paths"]
    if not isinstance(output_paths, list) or not all(
        isinstance(path, str) and path for path in output_paths
    ):
        raise ValueError("Ownership inventory output_paths must be non-empty strings.")
    if output_paths != sorted(set(output_paths)):
        raise ValueError("Ownership inventory output_paths must be sorted and unique.")

    created_at = payload["created_at"]
    if not isinstance(created_at, str) or not _CREATED_AT_RE.match(created_at):
        raise ValueError("Ownership inventory created_at must be UTC RFC 3339 with a trailing Z.")

    return dict(payload)


def _normalized(path: str) -> str:
    text = path.replace("\\", "/")
    if "://" in text:
        scheme, _, rest = text.partition("://")
        return f"{scheme}://{posixpath.normpath(rest)}".rstrip("/")
    return posixpath.normpath(text).rstrip("/")


def _is_strict_child(candidate: str, root: str) -> bool:
    normalized_candidate = _normalized(candidate)
    normalized_root = _normalized(root)
    if not normalized_root or normalized_candidate == normalized_root:
        return False
    if ".." in normalized_candidate.split("/"):
        return False
    return normalized_candidate.startswith(f"{normalized_root}/")


@dataclass(frozen=True)
class ReconciliationFacts:
    """Immutable structured facts describing one reconciliation pass."""

    pipeline: str
    run_id: str
    selective: bool
    pruned_paths: tuple[str, ...]
    inventory_path: str
    inventory_replaced: bool


class OwnershipPlugin:
    """Singleton ownership capability backed by an explicit metadata root.

    Core owns execution and passes complete bound-plan output facts; this
    plugin owns the inventory lifecycle: derive deletion candidates only from
    a prior valid inventory, verify managed-root containment before deleting
    anything, delete idempotently, and commit the new inventory last.
    """

    def __init__(
        self,
        *,
        metadata_root: str | Path,
        storage_options: Mapping[str, Any] | None = None,
        plugin_id: str = "ownership",
    ) -> None:
        root_text = to_storage_string(metadata_root).strip() if metadata_root else ""
        if not root_text:
            raise ValueError(
                "OwnershipPlugin requires an explicit metadata_root; no default "
                "is derived from the pipeline or run root."
            )
        self.plugin_id = plugin_id
        self._metadata_root = root_text
        self._storage_options = dict(storage_options) if storage_options is not None else None
        self.last_reconciliation: ReconciliationFacts | None = None

    # -- inventory store ----------------------------------------------------

    def inventory_path(self, pipeline: str) -> str:
        return join_path(
            self._metadata_root, _SCHEMA_DIRNAME, _INVENTORIES_DIRNAME, f"{pipeline}.json"
        )

    def read_inventory(self, pipeline: str) -> dict[str, Any] | None:
        """Return the recorded inventory, or None when there is no valid basis.

        A missing inventory or a malformed schema-version-1 inventory means no
        deletion basis. An unsupported schema version raises.
        """
        fs, resolved = resolve_fs(self.inventory_path(pipeline), self._storage_options)
        if not fs.exists(resolved):
            return None

        try:
            with fs.open(resolved, mode="rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError):
            return None

        try:
            return validate_inventory(payload, expected_pipeline=pipeline)
        except UnsupportedInventoryVersionError:
            raise
        except ValueError:
            return None

    def _write_inventory(self, pipeline: str, inventory: Mapping[str, Any]) -> None:
        validated = validate_inventory(dict(inventory), expected_pipeline=pipeline)
        record_path = self.inventory_path(pipeline)
        fs, resolved = resolve_fs(record_path, self._storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
            json.dump(validated, handle, ensure_ascii=False, indent=2)
        reread = self.read_inventory(pipeline)
        if reread != validated:
            raise OwnershipIntegrityError(
                f"Ownership inventory read-back at '{record_path}' did not match "
                "the published inventory."
            )

    # -- OwnershipCapability --------------------------------------------------

    def reconcile(self, context: OwnershipContext) -> None:
        """Prune stale managed output on full runs and commit the new inventory last."""
        pipeline = context.run.pipeline_name

        if context.run.is_selective:
            self.last_reconciliation = ReconciliationFacts(
                pipeline=pipeline,
                run_id=context.run.run_id,
                selective=True,
                pruned_paths=(),
                inventory_path=self.inventory_path(pipeline),
                inventory_replaced=False,
            )
            return

        current_paths = sorted(
            {
                _normalized(output.step_dir)
                for output in context.outputs
                if output.persist and output.managed
            }
        )
        managed_root = _normalized(context.managed_root)

        prior = self.read_inventory(pipeline)
        candidates: list[str] = []
        if prior is not None and _normalized(prior["managed_root"]) == managed_root:
            current_set = set(current_paths)
            candidates = [
                path for path in prior["output_paths"] if _normalized(path) not in current_set
            ]

        for candidate in candidates:
            if not _is_strict_child(candidate, managed_root):
                raise OwnershipIntegrityError(
                    f"Ownership inventory candidate '{candidate}' escapes the managed "
                    f"root '{managed_root}'; aborting without deletion."
                )

        _fire_failpoint("before_deletion")
        pruned: list[str] = []
        for candidate in candidates:
            fs, resolved = resolve_fs(candidate, self._storage_options)
            if fs.exists(resolved):
                fs.rm(resolved, recursive=True)
                pruned.append(candidate)
            _fire_failpoint("during_deletion")

        _fire_failpoint("before_inventory_commit")
        inventory = {
            "schema_version": INVENTORY_SCHEMA_VERSION,
            "pipeline": pipeline,
            "run_id": context.run.run_id,
            "managed_root": managed_root,
            "output_paths": current_paths,
            "created_at": _utc_created_at(),
        }
        self._write_inventory(pipeline, inventory)

        self.last_reconciliation = ReconciliationFacts(
            pipeline=pipeline,
            run_id=context.run.run_id,
            selective=False,
            pruned_paths=tuple(pruned),
            inventory_path=self.inventory_path(pipeline),
            inventory_replaced=True,
        )


__all__ = [
    "INVENTORY_SCHEMA_VERSION",
    "OwnershipIntegrityError",
    "OwnershipPlugin",
    "ReconciliationFacts",
    "UnsupportedInventoryVersionError",
    "validate_inventory",
]
