"""Durable execution: the recovery store, its codecs, and the engine."""

from __future__ import annotations

from .codecs import (
    JSON_UTF8,
    JSON_UTF8_CODEC,
    CodecError,
    SnapshotCodec,
    SnapshotCodecs,
)
from .execution import Invocation, RunResult
from ..options import COMMIT_SCOPES, SCHEMA_VERSION, CommitScope, DurabilityOptions
from .ownership import ExecutorBusyError, ExecutorOwnership
from .sources import InMemorySource, SourceError, as_source, load_initial_value
from .storage import OutputLocation, database_path, resolve_state_dir
from .store import RecoveryStore, StepRecord, StructureMismatchError
from .structure import DeclaredStage, DeclaredStep, DeclaredStructure, declare
from .types import (
    DURABLE_SCHEMA_VERSION,
    DurabilityError,
    InvalidTransitionError,
    OutputIntegrityError,
    StoreUnavailableError,
    UnsupportedStoreVersionError,
)

__all__ = [
    "COMMIT_SCOPES",
    "DURABLE_SCHEMA_VERSION",
    "JSON_UTF8",
    "JSON_UTF8_CODEC",
    "SCHEMA_VERSION",
    "CodecError",
    "CommitScope",
    "DeclaredStage",
    "DeclaredStep",
    "DeclaredStructure",
    "DurabilityError",
    "DurabilityOptions",
    "ExecutorBusyError",
    "ExecutorOwnership",
    "InMemorySource",
    "InvalidTransitionError",
    "Invocation",
    "OutputIntegrityError",
    "OutputLocation",
    "RecoveryStore",
    "RunResult",
    "SnapshotCodec",
    "SnapshotCodecs",
    "SourceError",
    "StepRecord",
    "StoreUnavailableError",
    "StructureMismatchError",
    "UnsupportedStoreVersionError",
    "as_source",
    "database_path",
    "declare",
    "load_initial_value",
    "resolve_state_dir",
]
