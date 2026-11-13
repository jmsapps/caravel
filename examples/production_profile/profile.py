"""The production profile for durable execution.

Consolidation moved most of what used to be a plugin into the engine. The
authoritative progress store now owns committed-output evidence, cross-run
inventory, and worker leases, so a production profile is two decisions:

1. where durable state and artifacts live, and
2. which observers record lifecycle evidence.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from caravel import DurableStorage, ExecutionPolicy, LeasePolicy
from caravel.plugins.run_evidence import RunEvidencePlugin
from caravel.storage import join_path


def build_production_storage(
    state_root: str | Path,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> DurableStorage:
    """Durable storage for a production job.

    ``state_root`` must be a local filesystem path that survives invocations.
    The authoritative store is not supported on network filesystems or
    ephemeral disk; artifact storage is configured separately and may be remote.
    """
    root = str(state_root).strip()
    if not root:
        raise ValueError("state_root must be a non-empty path")
    return DurableStorage.at(root, storage_options=storage_options)


def build_production_plugins(
    metadata_root: str | Path,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> list[object]:
    """Build the explicit observer composition used for qualification.

    Checkpoint, ownership, and lease plugins are gone: the durable store
    provides all three, and a plugin can no longer contradict it.
    """
    root = str(metadata_root).strip()
    if not root:
        raise ValueError("metadata_root must be a non-empty path or fsspec URL")

    return [
        RunEvidencePlugin(
            metadata_root=join_path(root, "evidence"),
            storage_options=storage_options,
        )
    ]


def build_production_policy(*, workers: int = 1) -> ExecutionPolicy:
    """Concurrency policy. More than one worker requires fenced leases."""
    if workers == 1:
        return ExecutionPolicy()
    return ExecutionPolicy(
        workers=workers, leases=LeasePolicy(ttl_seconds=60, heartbeat_seconds=10)
    )
