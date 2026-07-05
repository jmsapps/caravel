"""First-party run-evidence plugin: durable sanitized run diagnostics.

``RunEvidencePlugin`` is an observer-capability plugin. It records one
immutable schema-versioned event object per committed lifecycle transition:

    <metadata_root>/v1/events/<run_id>/<seq>-<kind>.json
    <metadata_root>/v1/summaries/<run_id>.json

``<seq>`` is a zero-padded per-run in-process counter, so the event sequence
is deterministic within one process. Events carry identifiers and facts only:
run and node IDs, indexes, names, dataset type and name, output paths, and
exception class names. They never contain payloads, credentials, storage
options, environment values, or parameter values. Core reduces exceptions to
class names before events reach observers, so no exception message text lands
in evidence.

The summary is a regenerable convenience derived exclusively from the run's
event objects; nothing reads a summary to make an execution or recovery
decision, so summary corruption cannot affect recovery or execution. Checkpoint
and ownership state is referenced only through event facts (a ``node_skipped``
event names the checkpoint-blessed node); this plugin never reads or writes
those stores. Removing the plugin removes history, not execution behavior.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from ..logger import get_logger
from ..storage import ensure_parent_dir, join_path, leaf_name, resolve_fs, to_storage_string
from .api import ObserverCriticality, RunEvent

RUN_EVENT_SCHEMA_VERSION = 1
_SCHEMA_DIRNAME = f"v{RUN_EVENT_SCHEMA_VERSION}"
_EVENTS_DIRNAME = "events"
_SUMMARIES_DIRNAME = "summaries"

_EVENT_KINDS = frozenset(
    {
        "run_started",
        "node_started",
        "node_completed",
        "node_failed",
        "node_skipped",
        "run_completed",
        "run_failed",
    }
)

_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_CREATED_AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")

_EVENT_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "pipeline",
        "is_selective",
        "run_root",
        "sequence",
        "kind",
        "node",
        "error_type",
        "created_at",
    }
)

_NODE_FIELDS = frozenset(
    {
        "node_id",
        "kind",
        "stage_index",
        "stage_name",
        "entry_index",
        "name",
        "persist",
        "dataset_type",
        "dataset_name",
        "step_dir",
        "route_key",
    }
)

# Deterministic failure injection for run-evidence contract tests. Production
# code leaves the hook unset; tests assign a callable that raises at a chosen
# failpoint name.
_failpoint_hook: Callable[[str], None] | None = None


def _fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


def _utc_created_at() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class RunEvidenceIntegrityError(Exception):
    """Raised when recorded run evidence is corrupt, missing, or inconsistent."""


class UnsupportedRunEventVersionError(Exception):
    """Raised when an event was written by an unsupported schema version."""


def validate_run_event(payload: object) -> dict[str, Any]:
    """Strictly validate a schema-version-1 run event object.

    Raises UnsupportedRunEventVersionError for a non-1 integer schema version
    and ValueError for anything malformed under schema version 1.
    """
    if not isinstance(payload, dict):
        raise ValueError("Run event must be a JSON object.")

    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ValueError("Run event schema_version must be an integer.")
    if schema_version != RUN_EVENT_SCHEMA_VERSION:
        raise UnsupportedRunEventVersionError(
            f"Unsupported run event schema version {schema_version}; "
            f"supported version is {RUN_EVENT_SCHEMA_VERSION}."
        )

    if set(payload) != _EVENT_FIELDS:
        unknown = sorted(set(payload) - _EVENT_FIELDS)
        missing = sorted(_EVENT_FIELDS - set(payload))
        raise ValueError(f"Run event fields invalid; unknown={unknown} missing={missing}.")

    run_id = payload["run_id"]
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        raise ValueError("Run event run_id must be lowercase UUID4 hex.")

    pipeline = payload["pipeline"]
    if not isinstance(pipeline, str) or not pipeline:
        raise ValueError("Run event pipeline must be a non-empty string.")

    if not isinstance(payload["is_selective"], bool):
        raise ValueError("Run event is_selective must be a boolean.")

    run_root = payload["run_root"]
    if not isinstance(run_root, str) or not run_root:
        raise ValueError("Run event run_root must be a non-empty string.")

    sequence = payload["sequence"]
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ValueError("Run event sequence must be a positive integer.")

    kind = payload["kind"]
    if kind not in _EVENT_KINDS:
        raise ValueError(f"Run event kind '{kind}' is not supported.")

    node = payload["node"]
    if node is not None:
        if not isinstance(node, dict) or set(node) != _NODE_FIELDS:
            raise ValueError("Run event node must carry exactly the node fact fields.")
        if not isinstance(node["node_id"], str) or not node["node_id"]:
            raise ValueError("Run event node_id must be a non-empty string.")

    error_type = payload["error_type"]
    if error_type is not None and (not isinstance(error_type, str) or not error_type):
        raise ValueError("Run event error_type must be null or a non-empty string.")

    created_at = payload["created_at"]
    if not isinstance(created_at, str) or not _CREATED_AT_RE.match(created_at):
        raise ValueError("Run event created_at must be UTC RFC 3339 with a trailing Z.")

    return dict(payload)


class RunEvidencePlugin:
    """Observer plugin that persists immutable, sanitized run and node events.

    ``criticality='required'`` (the default) makes an evidence write failure a
    distinct operational failure at the next safe boundary without invalidating
    already committed checkpoints; ``'best_effort'`` surfaces failures in logs
    and ``RunResult.best_effort_errors`` without changing execution
    correctness.
    """

    def __init__(
        self,
        *,
        metadata_root: str | Path,
        storage_options: Mapping[str, Any] | None = None,
        criticality: ObserverCriticality = "required",
        plugin_id: str = "run-evidence",
    ) -> None:
        root_text = to_storage_string(metadata_root).strip() if metadata_root else ""
        if not root_text:
            raise ValueError(
                "RunEvidencePlugin requires an explicit metadata_root; no default "
                "is derived from the pipeline or run root."
            )
        if criticality not in ("required", "best_effort"):
            raise ValueError("RunEvidencePlugin criticality must be 'required' or 'best_effort'.")
        self.plugin_id = plugin_id
        self.criticality: ObserverCriticality = criticality
        self._metadata_root = root_text
        self._storage_options = dict(storage_options) if storage_options is not None else None
        self._sequences: dict[str, int] = {}
        self._logger = get_logger("caravel.plugins.run_evidence", log_name="run_evidence")

    # -- evidence store ------------------------------------------------------

    def events_dir(self, run_id: str) -> str:
        return join_path(self._metadata_root, _SCHEMA_DIRNAME, _EVENTS_DIRNAME, run_id)

    def summary_path(self, run_id: str) -> str:
        return join_path(self._metadata_root, _SCHEMA_DIRNAME, _SUMMARIES_DIRNAME, f"{run_id}.json")

    def read_events(self, run_id: str) -> list[dict[str, Any]]:
        """Read, validate, and sequence-order every event recorded for a run."""
        events_dir = self.events_dir(run_id)
        fs, resolved = resolve_fs(events_dir, self._storage_options)
        if not fs.exists(resolved):
            return []

        events: list[dict[str, Any]] = []
        for file_path in sorted(str(path) for path in fs.find(resolved, withdirs=False)):
            if not file_path.endswith(".json"):
                continue
            try:
                with fs.open(file_path, mode="rt", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, ValueError) as read_error:
                raise RunEvidenceIntegrityError(
                    f"Run event object '{leaf_name(file_path)}' for run '{run_id}' "
                    "is unreadable or corrupt."
                ) from read_error
            try:
                events.append(validate_run_event(payload))
            except UnsupportedRunEventVersionError:
                raise
            except ValueError as validation_error:
                raise RunEvidenceIntegrityError(
                    f"Run event object '{leaf_name(file_path)}' for run '{run_id}' is malformed."
                ) from validation_error

        events.sort(key=lambda event: int(event["sequence"]))
        expected = list(range(1, len(events) + 1))
        if [event["sequence"] for event in events] != expected:
            raise RunEvidenceIntegrityError(
                f"Run '{run_id}' event sequence is incomplete or duplicated."
            )
        return events

    def regenerate_summary(self, run_id: str) -> dict[str, Any]:
        """Deterministically rebuild the run summary from its event objects."""
        events = self.read_events(run_id)
        if not events:
            raise RunEvidenceIntegrityError(f"Run '{run_id}' has no recorded events.")

        kinds = [str(event["kind"]) for event in events]
        status = "unknown"
        if "run_failed" in kinds:
            status = "failed"
        elif "run_completed" in kinds:
            status = "completed"

        nodes: dict[str, dict[str, Any]] = {}
        for event in events:
            node = event["node"]
            if node is None:
                continue
            node_id = str(node["node_id"])
            outcome = {
                "node_started": "started",
                "node_completed": "completed",
                "node_failed": "failed",
                "node_skipped": "skipped",
            }.get(str(event["kind"]))
            if outcome is None:
                continue
            entry = nodes.setdefault(
                node_id,
                {"node_id": node_id, "name": node["name"], "outcome": "unknown"},
            )
            entry["outcome"] = outcome

        failure_types = sorted(
            {str(event["error_type"]) for event in events if event["error_type"] is not None}
        )

        summary = {
            "schema_version": RUN_EVENT_SCHEMA_VERSION,
            "run_id": run_id,
            "pipeline": events[0]["pipeline"],
            "status": status,
            "event_count": len(events),
            "nodes": [nodes[node_id] for node_id in sorted(nodes)],
            "failure_types": failure_types,
            "generated_at": _utc_created_at(),
        }

        _fire_failpoint("before_summary_write")
        summary_path = self.summary_path(run_id)
        fs, resolved = resolve_fs(summary_path, self._storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
        return summary

    # -- RunObserver -----------------------------------------------------------

    def on_event(self, event: RunEvent) -> None:
        run_id = event.run.run_id
        sequence = self._sequences.get(run_id, 0) + 1
        self._sequences[run_id] = sequence

        node_block: dict[str, Any] | None = None
        if event.node is not None:
            node_block = {
                "node_id": event.node.node_id,
                "kind": event.node.kind,
                "stage_index": event.node.stage_index,
                "stage_name": event.node.stage_name,
                "entry_index": event.node.entry_index,
                "name": event.node.name,
                "persist": event.node.persist,
                "dataset_type": event.node.dataset_type,
                "dataset_name": event.node.dataset_name,
                "step_dir": event.node.step_dir,
                "route_key": event.node.route_key,
            }

        record = validate_run_event(
            {
                "schema_version": RUN_EVENT_SCHEMA_VERSION,
                "run_id": run_id,
                "pipeline": event.run.pipeline_name,
                "is_selective": event.run.is_selective,
                "run_root": event.run.run_root,
                "sequence": sequence,
                "kind": event.kind,
                "node": node_block,
                "error_type": event.error_type,
                "created_at": _utc_created_at(),
            }
        )

        _fire_failpoint("before_event_write")
        event_path = join_path(self.events_dir(run_id), f"{sequence:06d}-{event.kind}.json")
        fs, resolved = resolve_fs(event_path, self._storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)
        _fire_failpoint("after_event_write")

        self._logger.info(
            "RUN EVENT kind=%s pipeline=%s run_id=%s node=%s error_type=%s",
            event.kind,
            event.run.pipeline_name,
            run_id,
            event.node.node_id if event.node is not None else None,
            event.error_type,
        )

        if event.kind in ("run_completed", "run_failed"):
            self.regenerate_summary(run_id)


__all__ = [
    "RUN_EVENT_SCHEMA_VERSION",
    "RunEvidenceIntegrityError",
    "RunEvidencePlugin",
    "UnsupportedRunEventVersionError",
    "validate_run_event",
]
