"""Durability configuration.

Supplying :class:`DurabilityOptions` to a ``Pipeline`` is what makes a run
durable. The options carry the job's identity and the commit timing; the output
location belongs to the invocation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SCHEMA_VERSION = "v1.0.0"

CommitScope = Literal["pipeline", "stage", "step"]

COMMIT_SCOPES: tuple[CommitScope, ...] = ("pipeline", "stage", "step")


@dataclass(frozen=True)
class DurabilityOptions:
    """Durability configuration for one pipeline.

    ``schema_version`` is stated explicitly so a declaration names the storage
    format it was written against. ``job_id`` identifies one logical run and
    comes from the caller: a CLI argument, a trigger payload, or any other
    source the caller already has. ``commit_scope`` selects when transactions
    commit, and defaults to committing once on successful pipeline completion.
    """

    schema_version: Literal["v1.0.0"]
    job_id: str
    commit_scope: CommitScope = "pipeline"

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"DurabilityOptions.schema_version must be {SCHEMA_VERSION!r}, got "
                f"{self.schema_version!r}. State the storage format explicitly."
            )

        if not isinstance(self.job_id, str) or not self.job_id.strip():
            raise ValueError(
                "DurabilityOptions.job_id must be a non-empty string: recovery is "
                "keyed on it. Supply the identity this run should resume under."
            )

        if self.commit_scope not in COMMIT_SCOPES:
            raise ValueError(
                f"DurabilityOptions.commit_scope must be one of "
                f"{', '.join(COMMIT_SCOPES)}, got {self.commit_scope!r}."
            )


__all__ = ["COMMIT_SCOPES", "SCHEMA_VERSION", "CommitScope", "DurabilityOptions"]
