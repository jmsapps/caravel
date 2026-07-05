"""First-party lease plugin: advisory writer-conflict evidence and heartbeats.

``LeasePlugin`` is a run-guard capability. It maintains one replaceable lease
object per pipeline beneath its required explicit ``metadata_root``:

    <metadata_root>/v1/leases/<pipeline>.json
    <metadata_root>/v1/recoveries/<pipeline>/<run_id>.json

The lease records holder identity, host, process ID, start time, and a
heartbeat time refreshed by a plugin-owned daemon thread. The thread only
rewrites lease evidence; it never executes user code or lifecycle callbacks.

This is advisory diagnostics, not a distributed lock: generic fsspec offers no
portable atomic create-if-absent, so two racing writers can both acquire.
Authoritative serialization of writers to one pipeline root, run-level
retries, and hard timeout belong to the external scheduler or container.

Failure semantics are explicit:

- an apparently live foreign lease refuses startup with ``LeaseHeldError``;
- a stale lease (heartbeat older than the stale threshold) is recovered: a
  recovery evidence object naming the abandoned run is written, then the
  lease is replaced — when ``RunEvidencePlugin`` is configured, the abandoned
  run's history is found in its event store under the recorded run ID;
- a malformed lease fails closed with ``LeaseIntegrityError`` for operator
  inspection; an unsupported schema version raises
  ``UnsupportedLeaseVersionError``;
- a mid-run heartbeat write failure stops heartbeating and surfaces at guard
  teardown as a distinct operational failure without changing run output; and
- normal shutdown stops and joins the heartbeat thread, then releases the
  lease, leaving no abandoned worker thread.

Killing the whole process leaves the lease in place with an aging heartbeat,
which the next invocation classifies as stale after the threshold.
"""

from __future__ import annotations

import json
import os
import re
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from ..logger import get_logger
from ..storage import ensure_parent_dir, join_path, resolve_fs, to_storage_string
from .api import RunFacts, RunOutcome

LEASE_SCHEMA_VERSION = 1
_SCHEMA_DIRNAME = f"v{LEASE_SCHEMA_VERSION}"
_LEASES_DIRNAME = "leases"
_RECOVERIES_DIRNAME = "recoveries"

_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

_LEASE_FIELDS = frozenset(
    {
        "schema_version",
        "pipeline",
        "run_id",
        "holder",
        "host",
        "pid",
        "started_at",
        "heartbeat_at",
    }
)

# Deterministic failure injection for lease contract tests. Production code
# leaves the hook unset; tests assign a callable that raises at a chosen
# failpoint name.
_failpoint_hook: Callable[[str], None] | None = None


def _fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(moment: datetime) -> str:
    return moment.strftime(_TIMESTAMP_FORMAT)


def _parse_timestamp(text: str) -> datetime:
    return datetime.strptime(text, _TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)


class LeaseError(Exception):
    """Base class for lease-evidence failures raised by the lease plugin."""


class LeaseHeldError(LeaseError):
    """Raised when an apparently live lease refuses a new writer."""


class LeaseIntegrityError(LeaseError):
    """Raised when lease evidence cannot be classified and needs an operator."""


class UnsupportedLeaseVersionError(LeaseError):
    """Raised when a lease was written by an unsupported schema version."""


def validate_lease(payload: object, *, expected_pipeline: str) -> dict[str, Any]:
    """Strictly validate a schema-version-1 lease object.

    Raises UnsupportedLeaseVersionError for a non-1 integer schema version and
    ValueError for anything malformed under schema version 1.
    """
    if not isinstance(payload, dict):
        raise ValueError("Lease must be a JSON object.")

    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ValueError("Lease schema_version must be an integer.")
    if schema_version != LEASE_SCHEMA_VERSION:
        raise UnsupportedLeaseVersionError(
            f"Unsupported lease schema version {schema_version}; "
            f"supported version is {LEASE_SCHEMA_VERSION}."
        )

    if set(payload) != _LEASE_FIELDS:
        unknown = sorted(set(payload) - _LEASE_FIELDS)
        missing = sorted(_LEASE_FIELDS - set(payload))
        raise ValueError(f"Lease fields invalid; unknown={unknown} missing={missing}.")

    pipeline = payload["pipeline"]
    if not isinstance(pipeline, str) or not pipeline:
        raise ValueError("Lease pipeline must be a non-empty string.")
    if pipeline != expected_pipeline:
        raise ValueError(
            f"Lease pipeline '{pipeline}' does not match expected '{expected_pipeline}'."
        )

    run_id = payload["run_id"]
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        raise ValueError("Lease run_id must be lowercase UUID4 hex.")

    for field in ("holder", "host"):
        value = payload[field]
        if not isinstance(value, str) or not value:
            raise ValueError(f"Lease {field} must be a non-empty string.")

    pid = payload["pid"]
    if not isinstance(pid, int) or isinstance(pid, bool) or pid < 1:
        raise ValueError("Lease pid must be a positive integer.")

    for field in ("started_at", "heartbeat_at"):
        value = payload[field]
        if not isinstance(value, str) or not _TIMESTAMP_RE.match(value):
            raise ValueError(f"Lease {field} must be UTC RFC 3339 with a trailing Z.")

    return dict(payload)


class LeasePlugin:
    """Run-guard plugin providing advisory lease evidence and heartbeats.

    ``stale_threshold`` must strictly exceed ``heartbeat_interval`` so a
    healthy writer can never be classified as abandoned between refreshes.
    """

    def __init__(
        self,
        *,
        metadata_root: str | Path,
        storage_options: Mapping[str, Any] | None = None,
        heartbeat_interval: float = 10.0,
        stale_threshold: float = 60.0,
        holder: str | None = None,
        plugin_id: str = "lease",
    ) -> None:
        root_text = to_storage_string(metadata_root).strip() if metadata_root else ""
        if not root_text:
            raise ValueError(
                "LeasePlugin requires an explicit metadata_root; no default "
                "is derived from the pipeline or run root."
            )
        if heartbeat_interval <= 0:
            raise ValueError("LeasePlugin heartbeat_interval must be positive.")
        if stale_threshold <= heartbeat_interval:
            raise ValueError("LeasePlugin stale_threshold must strictly exceed heartbeat_interval.")
        self.plugin_id = plugin_id
        self._metadata_root = root_text
        self._storage_options = dict(storage_options) if storage_options is not None else None
        self._heartbeat_interval = float(heartbeat_interval)
        self._stale_threshold = float(stale_threshold)
        self._host = socket.gethostname()
        self._pid = os.getpid()
        self._holder = holder if holder else f"{self._host}:{self._pid}"
        self._logger = get_logger("caravel.plugins.lease", log_name="lease")
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._heartbeat_error: str | None = None
        self._lease: dict[str, Any] | None = None

    # -- lease store ----------------------------------------------------------

    def lease_path(self, pipeline: str) -> str:
        return join_path(self._metadata_root, _SCHEMA_DIRNAME, _LEASES_DIRNAME, f"{pipeline}.json")

    def recovery_path(self, pipeline: str, run_id: str) -> str:
        return join_path(
            self._metadata_root, _SCHEMA_DIRNAME, _RECOVERIES_DIRNAME, pipeline, f"{run_id}.json"
        )

    def read_lease(self, pipeline: str) -> dict[str, Any] | None:
        """Return the recorded lease, raising when it cannot be classified."""
        fs, resolved = resolve_fs(self.lease_path(pipeline), self._storage_options)
        if not fs.exists(resolved):
            return None

        try:
            with fs.open(resolved, mode="rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError) as read_error:
            raise LeaseIntegrityError(
                f"Lease for pipeline '{pipeline}' at '{self.lease_path(pipeline)}' is "
                "unreadable; inspect and remove it manually if the writer is gone."
            ) from read_error

        try:
            return validate_lease(payload, expected_pipeline=pipeline)
        except UnsupportedLeaseVersionError:
            raise
        except ValueError as validation_error:
            raise LeaseIntegrityError(
                f"Lease for pipeline '{pipeline}' at '{self.lease_path(pipeline)}' is "
                "malformed; inspect and remove it manually if the writer is gone."
            ) from validation_error

    def _write_lease(self, pipeline: str, lease: Mapping[str, Any]) -> dict[str, Any]:
        validated = validate_lease(dict(lease), expected_pipeline=pipeline)
        fs, resolved = resolve_fs(self.lease_path(pipeline), self._storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
            json.dump(validated, handle, ensure_ascii=False, indent=2)
        return validated

    def _write_recovery_evidence(self, run: RunFacts, stale_lease: Mapping[str, Any]) -> None:
        record = {
            "schema_version": LEASE_SCHEMA_VERSION,
            "pipeline": run.pipeline_name,
            "recovered_by_run_id": run.run_id,
            "recovered_at": _format_timestamp(_utc_now()),
            "stale_threshold_seconds": self._stale_threshold,
            "abandoned_lease": dict(stale_lease),
        }
        recovery = self.recovery_path(run.pipeline_name, run.run_id)
        fs, resolved = resolve_fs(recovery, self._storage_options)
        ensure_parent_dir(fs, resolved)
        with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)

    # -- heartbeat thread -------------------------------------------------------

    def _heartbeat_loop(self, pipeline: str) -> None:
        while not self._stop.wait(self._heartbeat_interval):
            try:
                _fire_failpoint("before_heartbeat_write")
                lease = self._lease
                if lease is None:
                    return
                refreshed = dict(lease)
                refreshed["heartbeat_at"] = _format_timestamp(_utc_now())
                self._lease = self._write_lease(pipeline, refreshed)
            except Exception as heartbeat_error:
                self._heartbeat_error = type(heartbeat_error).__name__
                self._logger.error(
                    "LEASE HEARTBEAT FAILED pipeline=%s holder=%s error_type=%s",
                    pipeline,
                    self._holder,
                    type(heartbeat_error).__name__,
                )
                return

    def heartbeat_thread_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # -- RunGuard ---------------------------------------------------------------

    def enter(self, run: RunFacts) -> None:
        pipeline = run.pipeline_name
        existing = self.read_lease(pipeline)
        if existing is not None:
            heartbeat_age = (
                _utc_now() - _parse_timestamp(existing["heartbeat_at"])
            ).total_seconds()
            if heartbeat_age <= self._stale_threshold:
                raise LeaseHeldError(
                    f"Pipeline '{pipeline}' lease is apparently held by "
                    f"'{existing['holder']}' (run {existing['run_id']}); refusing to "
                    "start. External scheduling must serialize writers."
                )
            self._logger.error(
                "LEASE STALE RECOVERY pipeline=%s abandoned_run=%s abandoned_holder=%s",
                pipeline,
                existing["run_id"],
                existing["holder"],
            )
            self._write_recovery_evidence(run, existing)

        now = _format_timestamp(_utc_now())
        self._lease = self._write_lease(
            pipeline,
            {
                "schema_version": LEASE_SCHEMA_VERSION,
                "pipeline": pipeline,
                "run_id": run.run_id,
                "holder": self._holder,
                "host": self._host,
                "pid": self._pid,
                "started_at": now,
                "heartbeat_at": now,
            },
        )
        self._heartbeat_error = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(pipeline,),
            name=f"caravel-lease-heartbeat-{pipeline}",
            daemon=True,
        )
        self._thread.start()

    def exit(self, run: RunFacts, outcome: RunOutcome) -> None:
        _ = outcome
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(5.0, self._heartbeat_interval * 2))
            self._thread = None
        self._lease = None

        _fire_failpoint("before_lease_release")
        fs, resolved = resolve_fs(self.lease_path(run.pipeline_name), self._storage_options)
        if fs.exists(resolved):
            fs.rm(resolved)

        if self._heartbeat_error is not None:
            raise LeaseIntegrityError(
                f"Lease heartbeat for pipeline '{run.pipeline_name}' failed mid-run "
                f"({self._heartbeat_error}); lease evidence may have gone stale."
            )


__all__ = [
    "LEASE_SCHEMA_VERSION",
    "LeaseError",
    "LeaseHeldError",
    "LeaseIntegrityError",
    "LeasePlugin",
    "UnsupportedLeaseVersionError",
    "validate_lease",
]
