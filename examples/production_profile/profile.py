from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from caravel.plugins import (
    CheckpointPlugin,
    LeasePlugin,
    OwnershipPlugin,
    RunEvidencePlugin,
)
from caravel.storage import join_path


def build_production_plugins(
    metadata_root: str | Path,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> list[object]:
    """Build the explicit first-party plugin composition used for qualification."""
    root = str(metadata_root).strip()
    if not root:
        raise ValueError("metadata_root must be a non-empty path or fsspec URL")

    return [
        CheckpointPlugin(
            metadata_root=join_path(root, "checkpoints"),
            storage_options=storage_options,
        ),
        OwnershipPlugin(
            metadata_root=join_path(root, "ownership"),
            storage_options=storage_options,
        ),
        RunEvidencePlugin(
            metadata_root=join_path(root, "evidence"),
            storage_options=storage_options,
        ),
        LeasePlugin(
            metadata_root=join_path(root, "leases"),
            storage_options=storage_options,
            heartbeat_interval=10.0,
            stale_threshold=60.0,
        ),
    ]
