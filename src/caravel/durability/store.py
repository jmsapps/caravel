"""The v1 recovery store.

One SQLite database per pipeline holds every job the pipeline has run. A Step
row carries that Step's complete output snapshot and its status; Stage and run
rows carry status. Commit timing is the caller's choice, so this module writes
eagerly and commits when asked.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal

from ..options import CommitScope
from .structure import DeclaredStructure
from .types import DURABLE_SCHEMA_VERSION, DurabilityError, UnsupportedStoreVersionError

FORMAT_IDENTITY = "caravel-durability"

_FORMAT_KEY = "format"
_VERSION_KEY = "schema_version"

StepStatus = Literal["pending", "running", "completed", "skipped", "failed"]

SCHEMA_STATEMENTS = (
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE IF NOT EXISTS durability_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS runs ("
    "job_id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL, pipeline_name TEXT NOT NULL, "
    "commit_scope TEXT NOT NULL "
    "CHECK (commit_scope IN ('pipeline', 'stage', 'step')), "
    "status TEXT NOT NULL DEFAULT 'pending' "
    "CHECK (status IN ('pending', 'running', 'completed', 'failed')), "
    "created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS stages ("
    "job_id TEXT NOT NULL REFERENCES runs(job_id) ON DELETE CASCADE, "
    "stage_id TEXT NOT NULL, stage_name TEXT NOT NULL, stage_order INTEGER NOT NULL, "
    "status TEXT NOT NULL DEFAULT 'pending' "
    "CHECK (status IN ('pending', 'running', 'completed', 'failed')), "
    "updated_at TEXT NOT NULL, PRIMARY KEY (job_id, stage_id))",
    "CREATE UNIQUE INDEX IF NOT EXISTS stages_order ON stages (job_id, stage_order)",
    "CREATE TABLE IF NOT EXISTS steps ("
    "job_id TEXT NOT NULL, step_id TEXT NOT NULL, stage_id TEXT NOT NULL, "
    "step_name TEXT NOT NULL, step_order INTEGER NOT NULL, "
    "status TEXT NOT NULL DEFAULT 'pending' "
    "CHECK (status IN ('pending', 'running', 'completed', 'skipped', 'failed')), "
    "snapshot BLOB, codec_identity TEXT, route_key TEXT, updated_at TEXT NOT NULL, "
    "PRIMARY KEY (job_id, step_id), "
    "FOREIGN KEY (job_id, stage_id) REFERENCES stages(job_id, stage_id) ON DELETE CASCADE, "
    "CHECK ((status = 'completed') = (snapshot IS NOT NULL)), "
    "CHECK ((snapshot IS NULL) = (codec_identity IS NULL)))",
    "CREATE UNIQUE INDEX IF NOT EXISTS steps_order ON steps (job_id, stage_id, step_order)",
    "CREATE INDEX IF NOT EXISTS steps_unfinished ON steps (job_id, status, stage_id, step_order)",
)


class StructureMismatchError(DurabilityError):
    """Raised when a job's stored structure differs from the declaration."""


@dataclass(frozen=True)
class StepRecord:
    """One Step's durable state."""

    step_id: str
    stage_id: str
    step_name: str
    step_order: int
    status: StepStatus
    snapshot: bytes | None
    codec_identity: str | None
    route_key: str | None


class RecoveryStore:
    """The SQLite database holding a pipeline's jobs.

    The store writes inside one open transaction and commits when the caller's
    commit scope reaches a boundary. ``has_pending`` reports whether a commit
    would persist anything, so a boundary with nothing pending performs no
    commit.
    """

    def __init__(self, path: Path | str, *, timeout: float = 5.0) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, timeout=timeout, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._pending = False
        self._savepoints: list[str] = []
        self._prepare()

    # -- lifecycle ---------------------------------------------------------

    def _prepare(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")

        stored_format = self._metadata(_FORMAT_KEY)
        stored_version = self._metadata(_VERSION_KEY)

        if stored_format is not None and stored_format != FORMAT_IDENTITY:
            raise UnsupportedStoreVersionError(
                f"{self.path} holds format {stored_format!r}; this is a "
                f"{FORMAT_IDENTITY!r} store. Point at the right database, or "
                "recreate this one deliberately."
            )
        if stored_version is not None and stored_version != DURABLE_SCHEMA_VERSION:
            raise UnsupportedStoreVersionError(
                f"{self.path} holds schema version {stored_version!r}; this build "
                f"reads {DURABLE_SCHEMA_VERSION!r}. Recreate the database "
                "deliberately to move to this version."
            )

        for statement in SCHEMA_STATEMENTS:
            self._connection.execute(statement)
        self._connection.executemany(
            "INSERT OR IGNORE INTO durability_metadata (key, value) VALUES (?, ?)",
            ((_FORMAT_KEY, FORMAT_IDENTITY), (_VERSION_KEY, DURABLE_SCHEMA_VERSION)),
        )

    def _metadata(self, key: str) -> str | None:
        tables = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='durability_metadata'"
        ).fetchone()
        if tables is None:
            return None
        row = self._connection.execute(
            "SELECT value FROM durability_metadata WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row["value"])

    @property
    def schema_version(self) -> str:
        version = self._metadata(_VERSION_KEY)
        return DURABLE_SCHEMA_VERSION if version is None else version

    def close(self) -> None:
        self._connection.close()

    # -- transactions ------------------------------------------------------

    @property
    def has_pending(self) -> bool:
        """True when a commit would persist work."""
        return self._pending

    def begin(self) -> None:
        if not self._connection.in_transaction:
            self._connection.execute("BEGIN")

    def commit(self) -> bool:
        """Persist pending work. Returns True when a commit was performed."""
        if not self._pending:
            return False
        self._connection.execute("COMMIT")
        self._pending = False
        self._savepoints.clear()
        self.begin()
        return True

    def rollback(self) -> None:
        if self._connection.in_transaction:
            self._connection.execute("ROLLBACK")
        self._pending = False
        self._savepoints.clear()
        self.begin()

    def savepoint(self, name: str) -> None:
        """Open a savepoint, replacing any earlier savepoint of the same name."""
        self.begin()
        self._connection.execute(f"SAVEPOINT {name}")
        if name not in self._savepoints:
            self._savepoints.append(name)

    def rollback_to(self, name: str) -> None:
        """Discard work since ``name``.

        A commit invalidates open savepoints, so a savepoint taken before the
        latest commit is simply absent and the committed work stands.
        """
        if name not in self._savepoints:
            return
        self._connection.execute(f"ROLLBACK TO {name}")

    def release(self, name: str) -> None:
        if name in self._savepoints:
            self._connection.execute(f"RELEASE {name}")
            self._savepoints.remove(name)

    def _write(self, sql: str, params: Iterable[Any]) -> None:
        self.begin()
        self._connection.execute(sql, tuple(params))
        self._pending = True

    # -- structure ---------------------------------------------------------

    def open_run(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        pipeline_name: str,
        commit_scope: CommitScope,
        structure: DeclaredStructure,
        now: str,
    ) -> bool:
        """Record this job's structure, or confirm the stored one matches.

        Returns True when the job is resuming work a previous invocation
        committed.
        """
        existing = self._connection.execute(
            "SELECT job_id FROM runs WHERE job_id = ?", (job_id,)
        ).fetchone()

        if existing is None:
            self._write(
                "INSERT INTO runs (job_id, pipeline_id, pipeline_name, commit_scope, "
                "status, created_at, updated_at) VALUES (?, ?, ?, ?, 'running', ?, ?)",
                (job_id, pipeline_id, pipeline_name, commit_scope, now, now),
            )
            for stage in structure.stages:
                self._write(
                    "INSERT INTO stages (job_id, stage_id, stage_name, stage_order, "
                    "status, updated_at) VALUES (?, ?, ?, ?, 'pending', ?)",
                    (job_id, stage.stage_id, stage.stage_name, stage.stage_order, now),
                )
                for step in stage.steps:
                    self._write(
                        "INSERT INTO steps (job_id, step_id, stage_id, step_name, "
                        "step_order, status, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                        (job_id, step.step_id, stage.stage_id, step.name, step.step_order, now),
                    )
            return False

        self._verify_structure(job_id, structure)
        self._write(
            "UPDATE runs SET status = 'running', updated_at = ? WHERE job_id = ?",
            (now, job_id),
        )
        return True

    def _verify_structure(self, job_id: str, structure: DeclaredStructure) -> None:
        stored = tuple(
            (
                str(row["stage_id"]),
                int(row["stage_order"]),
                str(row["step_id"]),
                int(row["step_order"]),
            )
            for row in self._connection.execute(
                "SELECT s.stage_id, g.stage_order, s.step_id, s.step_order "
                "FROM steps s JOIN stages g ON g.job_id = s.job_id AND g.stage_id = s.stage_id "
                "WHERE s.job_id = ? ORDER BY g.stage_order, s.step_order",
                (job_id,),
            )
        )
        declared = structure.signature()
        if stored != declared:
            raise StructureMismatchError(
                f"Job {job_id!r} was stored with a different declared structure. "
                "Recovery matches declared Stage and Step identities and order. "
                "Run the matching pipeline, or choose a new job ID."
            )

    def run_status(self, job_id: str) -> str | None:
        row = self._connection.execute(
            "SELECT status FROM runs WHERE job_id = ?", (job_id,)
        ).fetchone()
        return None if row is None else str(row["status"])

    # -- step state --------------------------------------------------------

    def steps(self, job_id: str) -> dict[str, StepRecord]:
        return {
            str(row["step_id"]): StepRecord(
                step_id=str(row["step_id"]),
                stage_id=str(row["stage_id"]),
                step_name=str(row["step_name"]),
                step_order=int(row["step_order"]),
                status=str(row["status"]),  # type: ignore[arg-type]
                snapshot=row["snapshot"],
                codec_identity=row["codec_identity"],
                route_key=row["route_key"],
            )
            for row in self._connection.execute(
                "SELECT * FROM steps WHERE job_id = ? ORDER BY stage_id, step_order",
                (job_id,),
            )
        }

    def complete_step(
        self,
        *,
        job_id: str,
        step_id: str,
        snapshot: bytes,
        codec_identity: str,
        now: str,
        route_key: str | None = None,
    ) -> None:
        """Write a Step's snapshot and its completion together."""
        self._write(
            "UPDATE steps SET status = 'completed', snapshot = ?, codec_identity = ?, "
            "route_key = COALESCE(?, route_key), updated_at = ? "
            "WHERE job_id = ? AND step_id = ?",
            (snapshot, codec_identity, route_key, now, job_id, step_id),
        )

    def skip_step(self, *, job_id: str, step_id: str, now: str) -> None:
        self._write(
            "UPDATE steps SET status = 'skipped', snapshot = NULL, "
            "codec_identity = NULL, updated_at = ? WHERE job_id = ? AND step_id = ?",
            (now, job_id, step_id),
        )

    def reset_step(self, *, job_id: str, step_id: str, now: str) -> None:
        self._write(
            "UPDATE steps SET status = 'pending', snapshot = NULL, "
            "codec_identity = NULL, updated_at = ? WHERE job_id = ? AND step_id = ?",
            (now, job_id, step_id),
        )

    def fail_step(self, *, job_id: str, step_id: str, now: str) -> None:
        self._write(
            "UPDATE steps SET status = 'failed', snapshot = NULL, "
            "codec_identity = NULL, updated_at = ? WHERE job_id = ? AND step_id = ?",
            (now, job_id, step_id),
        )

    def complete_stage(self, *, job_id: str, stage_id: str, now: str) -> None:
        self._write(
            "UPDATE stages SET status = 'completed', updated_at = ? "
            "WHERE job_id = ? AND stage_id = ?",
            (now, job_id, stage_id),
        )

    def complete_run(self, *, job_id: str, now: str) -> None:
        self._write(
            "UPDATE runs SET status = 'completed', updated_at = ? WHERE job_id = ?",
            (now, job_id),
        )

    def fail_run(self, *, job_id: str, now: str) -> None:
        self._write(
            "UPDATE runs SET status = 'failed', updated_at = ? WHERE job_id = ?",
            (now, job_id),
        )

    def __enter__(self) -> RecoveryStore:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


__all__ = [
    "FORMAT_IDENTITY",
    "SCHEMA_STATEMENTS",
    "RecoveryStore",
    "StepRecord",
    "StepStatus",
    "StructureMismatchError",
]
