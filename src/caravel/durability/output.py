"""Immutable attempt output and manifest-backed loading.

Durable execution separates two concerns. An *attempt* writes a payload to a
unique, isolated location and verifies that it persisted. Only afterwards does a
conditional transaction publish an :class:`OutputReference` to that location.
Readers see only output reachable from a committed reference; unreferenced
attempt output is inert and may be collected later, so correctness never depends
on cleanup.

Execution identity lives entirely in the attempt path. A dataset's own storage
partition keys live inside that path, so the two namespaces cannot collide.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

from ..datasets import (
    CheckpointInspectableDataset,
    CheckpointLoadableDataset,
    CommittedEmptyLoadableDataset,
)
from ..storage import StoragePath, join_path, remove_and_recreate_dir, resolve_fs
from ..types import Dataset
from .types import (
    OutputIntegrityError,
    WorkUnitKey,
    validate_identifier,
    validate_opaque,
)

OUTPUT_REFERENCE_SCHEMA_VERSION = 1

ATTEMPTS_DIRNAME = "attempts"

# Longest encoded path segment kept verbatim. Longer identities are shortened to
# a bounded, deterministic, collision-resistant form.
_MAX_SEGMENT_LENGTH = 96
_SEGMENT_PREFIX_LENGTH = 40

_REFERENCE_FIELDS = frozenset({"schema_version", "path", "dataset", "partition_keys", "count"})


def encode_segment(value: str) -> str:
    """Return ``value`` as one safe, bounded, collision-free path segment.

    Percent-encoding every reserved character means an execution identity
    containing ``/`` can neither create nested directories nor traverse upward,
    and two distinct identities can never encode to the same segment. The
    relative-path names ``.`` and ``..`` survive percent-encoding unchanged, so
    they are replaced by a hashed form rather than written literally.
    """
    validate_identifier("path segment", value)
    encoded = quote(value, safe="")
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    # quote() leaves "." unreserved, so "." and ".." survive encoding intact and
    # would become real traversal segments. Hash those instead.
    if encoded in (".", ".."):
        return f"~{digest}"
    if len(encoded) > _MAX_SEGMENT_LENGTH:
        return f"{encoded[:_SEGMENT_PREFIX_LENGTH]}~{digest}"
    return encoded


def new_attempt_token() -> str:
    """Return a fresh attempt token, unique to one write attempt."""
    return uuid.uuid4().hex


class OutputReference:
    """An immutable, committed pointer to one work unit's batch output."""

    __slots__ = ("path", "dataset_type", "dataset_name", "partition_keys", "count")

    def __init__(
        self,
        *,
        path: str,
        dataset_type: str,
        dataset_name: str,
        partition_keys: list[str] | None,
        count: int,
    ) -> None:
        self.path = validate_opaque("output reference path", path)
        self.dataset_type = validate_identifier("output reference dataset type", dataset_type)
        if not isinstance(dataset_name, str):
            raise ValueError("Output reference dataset name must be a string.")
        self.dataset_name = dataset_name
        if partition_keys is not None:
            if not isinstance(partition_keys, list) or not all(
                isinstance(key, str) and key for key in partition_keys
            ):
                raise ValueError(
                    "Output reference partition_keys must be null or non-empty strings."
                )
            if partition_keys != sorted(set(partition_keys)):
                raise ValueError("Output reference partition_keys must be sorted and unique.")
        self.partition_keys = partition_keys
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("Output reference count must be a non-negative integer.")
        if partition_keys is None:
            if count != 1:
                raise ValueError("Output reference count must be 1 for a single-file dataset.")
        elif count != len(partition_keys):
            raise ValueError("Output reference count must equal the number of partition keys.")
        self.count = count

    @property
    def committed_empty(self) -> bool:
        """Whether this reference proves a committed empty partitioned payload."""
        return self.partition_keys == []

    def to_json(self) -> str:
        """Serialize for the store's opaque output reference column."""
        return json.dumps(
            {
                "schema_version": OUTPUT_REFERENCE_SCHEMA_VERSION,
                "path": self.path,
                "dataset": {"type": self.dataset_type, "name": self.dataset_name},
                "partition_keys": self.partition_keys,
                "count": self.count,
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, payload: str) -> OutputReference:
        """Parse and strictly validate a committed output reference."""
        try:
            decoded: Any = json.loads(payload)
        except ValueError as error:
            raise OutputIntegrityError("Committed output reference is not valid JSON.") from error
        if not isinstance(decoded, dict):
            raise OutputIntegrityError("Committed output reference must be a JSON object.")
        if set(decoded) != _REFERENCE_FIELDS:
            unknown = sorted(set(decoded) - _REFERENCE_FIELDS)
            missing = sorted(_REFERENCE_FIELDS - set(decoded))
            raise OutputIntegrityError(
                f"Committed output reference fields invalid; unknown={unknown} missing={missing}."
            )
        version = decoded["schema_version"]
        if not isinstance(version, int) or isinstance(version, bool):
            raise OutputIntegrityError("Output reference schema_version must be an integer.")
        if version != OUTPUT_REFERENCE_SCHEMA_VERSION:
            raise OutputIntegrityError(
                f"Unsupported output reference schema version {version}; supported "
                f"version is {OUTPUT_REFERENCE_SCHEMA_VERSION}."
            )
        dataset = decoded["dataset"]
        if not isinstance(dataset, dict) or set(dataset) != {"type", "name"}:
            raise OutputIntegrityError(
                "Output reference dataset must contain exactly type and name."
            )
        try:
            return cls(
                path=decoded["path"],
                dataset_type=dataset["type"],
                dataset_name=dataset["name"],
                partition_keys=decoded["partition_keys"],
                count=decoded["count"],
            )
        except ValueError as error:
            raise OutputIntegrityError(f"Committed output reference is invalid: {error}") from error

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OutputReference):
            return NotImplemented
        return (
            self.path == other.path
            and self.dataset_type == other.dataset_type
            and self.dataset_name == other.dataset_name
            and self.partition_keys == other.partition_keys
            and self.count == other.count
        )

    def __repr__(self) -> str:
        return (
            f"OutputReference(path={self.path!r}, dataset_type={self.dataset_type!r}, "
            f"count={self.count})"
        )


class AttemptOutputStore:
    """Writes isolated immutable attempt output and loads committed references.

    The store owns only locations and verification. It never decides whether an
    attempt becomes visible; that is the authoritative transaction's job.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        storage_options: Mapping[str, Any] | None = None,
    ) -> None:
        if root is None or not str(root).strip():
            raise ValueError("AttemptOutputStore requires an explicit output root.")
        self.root: StoragePath = str(root) if "://" in str(root) else Path(root)
        self.storage_options = dict(storage_options) if storage_options is not None else None

    def attempt_location(self, key: WorkUnitKey, batch_id: str, token: str) -> str:
        """Return the unique immutable location for one write attempt."""
        validate_identifier("batch_id", batch_id)
        validate_identifier("attempt token", token)
        return join_path(
            self.root,
            ATTEMPTS_DIRNAME,
            encode_segment(key.job_id),
            encode_segment(key.node_id),
            encode_segment(key.partition_id),
            encode_segment(batch_id),
            encode_segment(token),
        )

    def write_attempt(
        self,
        key: WorkUnitKey,
        batch_id: str,
        *,
        dataset: Dataset,
        payload: Any,
        token: str | None = None,
    ) -> OutputReference:
        """Persist one attempt's payload and verify it before returning a reference.

        The returned reference is not yet visible to readers. Only a committed
        transaction publishing it makes the output reachable.
        """
        attempt_token = new_attempt_token() if token is None else token
        destination = self.attempt_location(key, batch_id, attempt_token)

        remove_and_recreate_dir(destination, self.storage_options)
        dataset.save(payload, destination)

        partition_keys: list[str] | None = None
        if isinstance(dataset, CheckpointInspectableDataset):
            partition_keys = dataset.physical_partition_keys(destination)
            try:
                dataset.verify_physical_output(destination, partition_keys)
            except Exception as error:
                raise OutputIntegrityError(
                    f"Attempt output for node='{key.node_id}' partition='{key.partition_id}' "
                    f"batch='{batch_id}' could not be verified after writing."
                ) from error
        elif not dataset.exists(destination):
            raise OutputIntegrityError(
                f"Attempt output for node='{key.node_id}' partition='{key.partition_id}' "
                f"batch='{batch_id}' is absent after writing."
            )

        return OutputReference(
            path=destination,
            dataset_type=type(dataset).__name__,
            dataset_name=getattr(dataset, "name", ""),
            partition_keys=partition_keys,
            count=1 if partition_keys is None else len(partition_keys),
        )

    def load(self, reference: OutputReference, *, dataset: Dataset) -> Any:
        """Load committed output, validating it against the declared dataset.

        The dataset is supplied by the caller and checked against the reference's
        recorded metadata. A class is never resolved from stored text.
        """
        declared_type = type(dataset).__name__
        declared_name = getattr(dataset, "name", "")
        if reference.dataset_type != declared_type or reference.dataset_name != declared_name:
            raise OutputIntegrityError(
                f"Committed output was written by dataset "
                f"'{reference.dataset_type}'/'{reference.dataset_name}' but the current "
                f"declaration is '{declared_type}'/'{declared_name}'."
            )

        if reference.committed_empty:
            if not isinstance(dataset, CommittedEmptyLoadableDataset):
                raise OutputIntegrityError(
                    f"Dataset '{declared_type}' cannot reconstruct committed-empty output."
                )
            try:
                return dataset.load_committed_empty()
            except Exception as error:
                raise OutputIntegrityError(
                    "Committed-empty output could not be reconstructed."
                ) from error

        if not isinstance(dataset, CheckpointLoadableDataset):
            raise OutputIntegrityError(
                f"Dataset '{declared_type}' cannot load committed output; it does not "
                "implement load_from."
            )

        if isinstance(dataset, CheckpointInspectableDataset):
            try:
                dataset.verify_physical_output(reference.path, reference.partition_keys)
            except Exception as error:
                raise OutputIntegrityError(
                    f"Committed output at '{reference.path}' is missing or does not match "
                    "its manifest entry."
                ) from error

        try:
            return dataset.load_from(reference.path)
        except Exception as error:
            raise OutputIntegrityError(
                f"Committed output at '{reference.path}' could not be loaded."
            ) from error

    def exists(self, reference: OutputReference) -> bool:
        """Whether a referenced attempt location is physically present."""
        fs, resolved = resolve_fs(reference.path, self.storage_options)
        return bool(fs.exists(resolved))


__all__ = [
    "ATTEMPTS_DIRNAME",
    "OUTPUT_REFERENCE_SCHEMA_VERSION",
    "AttemptOutputStore",
    "OutputReference",
    "encode_segment",
    "new_attempt_token",
]
