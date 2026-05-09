"""Core type contracts for the Caravel pipeline framework.

This module intentionally keeps runtime behavior minimal and defines stable type
surfaces used by path/dataset/runner/branch modules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

PartitionId = str
Record = Any
Partitions = dict[PartitionId, Record]

SOURCE_FIELD = "__source__"


@dataclass(frozen=True)
class StepContext:
    """Execution context passed to every step.

    Notes:
        - Steps receive this as a keyword-only argument: ``context=...``.
        - Steps should not perform direct file I/O. Paths are exposed for logging
          and optional diagnostics only.
    """

    run_root: Path | str
    pipeline_name: str
    run_id: str
    stage_index: int
    stage_name: str
    step_index: int
    step_name: str
    step_dir: Path | str
    prev_step_dir: Path | str | None
    logger: logging.Logger
    params: Mapping[str, str]


# Deliberately permissive:
# - Input/output shape depends on the previous and current attached Datasets.
# - Runner enforces calling convention at runtime.
StepFn = Callable[..., Any]


@runtime_checkable
class Dataset(Protocol):
    """Dataset abstraction used for all pipeline I/O."""

    name: str

    def load(self) -> Any:
        """Load and return payload from the dataset's configured path."""
        ...

    def save(self, payload: Any, dest: Path | str) -> None:
        """Persist payload into destination folder ``dest``."""
        ...

    def exists(self, dest: Path | str) -> bool:
        """Return whether expected output already exists under ``dest``."""
        ...

    def describe(self) -> dict[str, Any]:
        """Return a small serializable description for logging/debug output."""
        ...


@runtime_checkable
class Loader(Protocol):
    """Input source that yields partitioned records."""

    name: str

    def load(self) -> Partitions:
        """Load source records keyed by partition id."""
        ...


BranchPredicate = Callable[[Record], str]


class KeyCollisionError(Exception):
    """Raised when two branches/sources emit the same partition key."""


class MissingSourceTagError(Exception):
    """Raised when source-aware routing is requested but ``__source__`` is missing."""


class MissingPriorOutputError(Exception):
    """Raised when selective execution requires prior output that is absent."""
