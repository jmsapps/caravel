"""One mapping between private execution identity and public output identity.

Execution identity is scheduling and recovery metadata. A step declaring
``inputs.all(...)`` becomes a single work unit on the reserved aggregate
partition, and that reserved ID belongs to authoritative state and to immutable
attempt artifacts. It is not an application-visible name.

Public output identity comes from the declaration instead: a node's resolved
dataset name, which defaults to the declared step name. Ordinary source
partitions keep the identities they already have, because those *are* declared
-- they come from the source, not from the scheduler.

Everything that addresses output publicly goes through this module: published
paths, ``results.of()``, ``results.published()``, the completion manifest, and
the batch feed. One mapping means a correction lands once rather than being
re-derived, and it means nothing infers identity by parsing a filename.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .sources import AGGREGATE_PARTITION_ID
from .types import DurabilityError

_UNSAFE_SEGMENTS = ("", ".", "..")


class LogicalIdentityError(DurabilityError):
    """Raised when declared output identities cannot address output unambiguously."""


def resolved_output_name(node: Any) -> str:
    """Return the declared name for one node's aggregate output.

    ``Step`` already defaults an unnamed dataset to the step's own name, so this
    reads the declaration rather than re-implementing a defaulting rule. The
    further fallbacks exist for internal nodes a declaration never names.
    """
    dataset_name = getattr(node.step.dataset, "name", "")
    if isinstance(dataset_name, str) and dataset_name:
        return dataset_name
    return str(node.step.name or node.node_id)


def validate_output_name(name: str, *, what: str) -> str:
    """Reject a declared output name that cannot address output on its own.

    Publication sanitises path segments, so this is not what keeps a separator
    out of a path. It is what keeps an unusable identity out of the public
    addressing namespace, and it runs before discovery or any durable mutation.
    """
    if not isinstance(name, str) or name in _UNSAFE_SEGMENTS:
        raise LogicalIdentityError(f"{what} must be a non-empty name, got {name!r}.")
    if "/" in name or "\\" in name:
        raise LogicalIdentityError(f"{what} '{name}' must not contain path separators.")
    return name


def logical_output_id(node: Any, partition_id: str) -> str:
    """Return the public identity of one work unit's output.

    A source partition addresses its own output. The reserved aggregate
    partition is private, so its output is addressed by the declaration that
    produced it.
    """
    if partition_id != AGGREGATE_PARTITION_ID:
        return partition_id
    return resolved_output_name(node)


@dataclass(frozen=True)
class LogicalOutputMap:
    """A node's bidirectional map between internal units and public identities.

    Built from work-unit identities the store knows about, so a reverse lookup
    resolves a public name to exactly one unit without parsing paths.
    """

    node_id: str
    to_logical: Mapping[str, str]
    to_partition: Mapping[str, str]

    @classmethod
    def for_node(cls, node: Any, partition_ids: Iterable[str]) -> LogicalOutputMap:
        forward: dict[str, str] = {}
        reverse: dict[str, str] = {}
        for partition_id in partition_ids:
            logical = logical_output_id(node, partition_id)
            if partition_id == AGGREGATE_PARTITION_ID:
                validate_output_name(logical, what=f"Output name for node '{node.node_id}'")
            if logical in reverse:
                raise LogicalIdentityError(
                    f"Node '{node.node_id}' would publish partitions "
                    f"'{reverse[logical]}' and '{partition_id}' both as '{logical}'. "
                    "Declare a distinct dataset name so output stays addressable."
                )
            forward[partition_id] = logical
            reverse[logical] = partition_id
        return cls(node_id=str(node.node_id), to_logical=forward, to_partition=reverse)


__all__ = [
    "LogicalIdentityError",
    "LogicalOutputMap",
    "logical_output_id",
    "resolved_output_name",
    "validate_output_name",
]
