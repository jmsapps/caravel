"""Transactional SQLite implementation of the durable execution store.

Supported envelope: one host, multiple processes, a local POSIX filesystem that
survives invocations. Network filesystems and ephemeral disk are not supported.
Multi-host metadata storage requires a separate implementation of the same
contract.

Every mutating transition opens ``BEGIN IMMEDIATE`` and validates ownership,
fence token, applicable lease expiry, and expected generation before writing, so
a read-validate-write sequence can never interleave with a competing writer.
"""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from types import TracebackType
from typing import Any, Callable, Iterator, Sequence, cast

from .store import validate_capabilities
from .types import (
    DURABLE_SCHEMA_VERSION,
    BatchRecord,
    ChangeOperation,
    DependencyEdge,
    DiscoveredPartition,
    FenceViolationError,
    GenerationConflictError,
    InvalidTransitionError,
    JobState,
    JobStatus,
    NodeSeal,
    PartitionRecord,
    PipelineCheckpoint,
    SourceMode,
    StoreCapabilities,
    StoreUnavailableError,
    UnsupportedStoreVersionError,
    WorkUnitKey,
    WorkUnitState,
    WorkUnitStatus,
    WorkUnitUnavailableError,
    validate_counter,
    validate_identifier,
    validate_opaque,
    validate_optional_opaque,
)

_SCHEMA_VERSION_KEY = "schema_version"

# SQLite builds carrying the WAL-reset fix. Outside these ranges WAL can lose
# committed frames on reset, so this store refuses WAL rather than claim a
# durability guarantee it cannot provide.
_WAL_FIXED_RANGES = (
    ((3, 51, 3), None),
    ((3, 50, 7), (3, 51, 0)),
    ((3, 44, 6), (3, 45, 0)),
)

_SCHEMA_STATEMENTS = (
    "CREATE TABLE IF NOT EXISTS publication_files ("
    "job_id TEXT NOT NULL, node_id TEXT NOT NULL, partition_id TEXT NOT NULL, "
    "output_generation INTEGER NOT NULL, files TEXT NOT NULL, "
    "PRIMARY KEY(job_id, node_id, partition_id, output_generation))",
    "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS jobs ("
    "job_id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL, lineage TEXT NOT NULL, "
    "source_mode TEXT NOT NULL, snapshot_id TEXT, range_start TEXT, range_end TEXT, "
    "discovery_position TEXT, discovery_sealed INTEGER NOT NULL DEFAULT 0, "
    "status TEXT NOT NULL DEFAULT 'open', declaration_fingerprint TEXT NOT NULL, "
    "generation INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, "
    "updated_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS pipeline_checkpoints ("
    "pipeline_id TEXT NOT NULL, lineage TEXT NOT NULL, completed_position TEXT, "
    "generation INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, "
    "PRIMARY KEY (pipeline_id, lineage))",
    "CREATE TABLE IF NOT EXISTS partitions ("
    "job_id TEXT NOT NULL, partition_id TEXT NOT NULL, input_ref TEXT NOT NULL, "
    "input_version TEXT, entity_key TEXT, operation TEXT, source_revision TEXT, "
    "source_position TEXT, discovered_at TEXT NOT NULL, "
    "PRIMARY KEY (job_id, partition_id))",
    "CREATE TABLE IF NOT EXISTS work_units ("
    "job_id TEXT NOT NULL, node_id TEXT NOT NULL, partition_id TEXT NOT NULL, "
    "status TEXT NOT NULL, input_scope TEXT NOT NULL, step_revision TEXT NOT NULL, "
    "params_fingerprint TEXT NOT NULL, cursor TEXT, "
    "batch_count INTEGER NOT NULL DEFAULT 0, "
    "input_exhausted INTEGER NOT NULL DEFAULT 0, "
    "pending_deps INTEGER NOT NULL DEFAULT 0, owner_id TEXT, "
    "claim_epoch INTEGER NOT NULL DEFAULT 0, fence_token INTEGER NOT NULL DEFAULT 0, "
    "lease_expires_at TEXT, generation INTEGER NOT NULL DEFAULT 0, sealed_at TEXT, "
    "error_type TEXT, route_key TEXT, output_generation INTEGER NOT NULL DEFAULT 0, "
    "published_generation INTEGER NOT NULL DEFAULT -1, "
    "PRIMARY KEY (job_id, node_id, partition_id))",
    "CREATE INDEX IF NOT EXISTS work_units_eligible ON work_units "
    "(job_id, status, pending_deps, partition_id, node_id)",
    "CREATE TABLE IF NOT EXISTS batches ("
    "position INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, "
    "node_id TEXT NOT NULL, partition_id TEXT NOT NULL, batch_id TEXT NOT NULL, "
    "output_ref TEXT NOT NULL, record_count INTEGER NOT NULL DEFAULT 0, "
    "committed_at TEXT NOT NULL, output_generation INTEGER NOT NULL DEFAULT 0, "
    "UNIQUE (job_id, node_id, partition_id, batch_id, output_generation))",
    "CREATE INDEX IF NOT EXISTS batches_feed ON batches (job_id, node_id, position)",
    "CREATE TABLE IF NOT EXISTS dependencies ("
    "job_id TEXT NOT NULL, node_id TEXT NOT NULL, partition_id TEXT NOT NULL, "
    "depends_on_node_id TEXT NOT NULL, depends_on_partition_id TEXT NOT NULL, "
    "satisfied INTEGER NOT NULL DEFAULT 0, "
    "PRIMARY KEY (job_id, node_id, partition_id, depends_on_node_id, "
    "depends_on_partition_id))",
    "CREATE INDEX IF NOT EXISTS dependencies_upstream ON dependencies "
    "(job_id, depends_on_node_id, depends_on_partition_id, satisfied)",
    "CREATE TABLE IF NOT EXISTS consumer_positions ("
    "pipeline_id TEXT NOT NULL, consumer_id TEXT NOT NULL, job_id TEXT NOT NULL, "
    "node_id TEXT NOT NULL, position INTEGER NOT NULL, "
    "generation INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, "
    "PRIMARY KEY (pipeline_id, consumer_id, job_id, node_id))",
    "CREATE TABLE IF NOT EXISTS retention ("
    "job_id TEXT PRIMARY KEY, floor_position INTEGER NOT NULL, updated_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS node_seals ("
    "job_id TEXT NOT NULL, node_id TEXT NOT NULL, final_position INTEGER, "
    "sealed_at TEXT NOT NULL, PRIMARY KEY (job_id, node_id))",
)

# Work a prior invocation left unfinished whose claim is demonstrably dead.
# This predicate must agree with ``claim_work_unit``: a unit is takeable when
# nobody owns it, or when its lease has an expiry that has passed. A claim with
# no expiry is treated as LIVE by both, so it is excluded here rather than
# offered up and then refused at claim time.
RECLAIMABLE_WORK_SQL = (
    "SELECT * FROM work_units "
    "WHERE job_id = ? AND status IN ('executing', 'failed') AND pending_deps = 0 "
    "AND (owner_id IS NULL "
    "     OR (lease_expires_at IS NOT NULL AND lease_expires_at <= ?)) "
    "ORDER BY partition_id, node_id LIMIT ?"
)

# The same query without the liveness filter, valid only when the caller is the
# sole worker and no other live holder can exist.
RECLAIMABLE_WORK_EXCLUSIVE_SQL = (
    "SELECT * FROM work_units "
    "WHERE job_id = ? AND status IN ('executing', 'failed') AND pending_deps = 0 "
    "ORDER BY partition_id, node_id LIMIT ?"
)

ELIGIBLE_WORK_SQL = (
    "SELECT * FROM work_units "
    "WHERE job_id = ? AND status = 'eligible' AND pending_deps = 0 "
    "ORDER BY partition_id, node_id LIMIT ?"
)

# Deterministic failure injection for durability contract tests. Production code
# leaves the hook unset; tests assign a callable that raises at a chosen
# failpoint name to prove transaction rollback.
_failpoint_hook: Callable[[str], None] | None = None


def _fire_failpoint(name: str) -> None:
    if _failpoint_hook is not None:
        _failpoint_hook(name)


def _wal_is_fixed(version: tuple[int, ...]) -> bool:
    for lower, upper in _WAL_FIXED_RANGES:
        if version >= lower and (upper is None or version < upper):
            return True
    return False


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_timestamp(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _text(row: sqlite3.Row, field: str) -> str:
    value = row[field]
    if not isinstance(value, str):
        raise InvalidTransitionError(f"Durable column '{field}' is corrupt: expected text.")
    return value


def _optional_text(row: sqlite3.Row, field: str) -> str | None:
    value = row[field]
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidTransitionError(f"Durable column '{field}' is corrupt: expected text or null.")
    return value


def _integer(row: sqlite3.Row, field: str) -> int:
    value = row[field]
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidTransitionError(f"Durable column '{field}' is corrupt: expected integer.")
    return value


def _optional_integer(row: sqlite3.Row, field: str) -> int | None:
    value = row[field]
    if value is None:
        return None
    return _integer(row, field)


def _flag(row: sqlite3.Row, field: str) -> bool:
    return _integer(row, field) != 0


def _job_from_row(row: sqlite3.Row) -> JobState:
    source_mode = _text(row, "source_mode")
    status = _text(row, "status")
    if source_mode not in ("snapshot", "incremental"):
        raise InvalidTransitionError(f"Durable job source_mode '{source_mode}' is not supported.")
    if status not in ("open", "completed"):
        raise InvalidTransitionError(f"Durable job status '{status}' is not supported.")
    mode: SourceMode = "snapshot" if source_mode == "snapshot" else "incremental"
    job_status: JobStatus = "open" if status == "open" else "completed"
    return JobState(
        job_id=_text(row, "job_id"),
        pipeline_id=_text(row, "pipeline_id"),
        lineage=_text(row, "lineage"),
        source_mode=mode,
        declaration_fingerprint=_text(row, "declaration_fingerprint"),
        snapshot_id=_optional_text(row, "snapshot_id"),
        range_start=_optional_text(row, "range_start"),
        range_end=_optional_text(row, "range_end"),
        discovery_position=_optional_text(row, "discovery_position"),
        discovery_sealed=_flag(row, "discovery_sealed"),
        status=job_status,
        generation=_integer(row, "generation"),
        created_at=_text(row, "created_at"),
        updated_at=_text(row, "updated_at"),
    )


def _work_unit_from_row(row: sqlite3.Row) -> WorkUnitState:
    scope = _text(row, "input_scope")
    status = _text(row, "status")
    if scope not in ("partition", "all"):
        raise InvalidTransitionError(f"Durable work unit input_scope '{scope}' is not supported.")
    if status not in ("eligible", "executing", "sealed", "failed"):
        raise InvalidTransitionError(f"Durable work unit status '{status}' is not supported.")
    return WorkUnitState(
        key=WorkUnitKey(
            job_id=_text(row, "job_id"),
            node_id=_text(row, "node_id"),
            partition_id=_text(row, "partition_id"),
        ),
        input_scope="partition" if scope == "partition" else "all",
        step_revision=_text(row, "step_revision"),
        params_fingerprint=_text(row, "params_fingerprint"),
        status=cast("WorkUnitStatus", status),
        cursor=_optional_text(row, "cursor"),
        batch_count=_integer(row, "batch_count"),
        input_exhausted=_flag(row, "input_exhausted"),
        pending_deps=_integer(row, "pending_deps"),
        owner_id=_optional_text(row, "owner_id"),
        claim_epoch=_integer(row, "claim_epoch"),
        fence_token=_integer(row, "fence_token"),
        lease_expires_at=_optional_text(row, "lease_expires_at"),
        generation=_integer(row, "generation"),
        sealed_at=_optional_text(row, "sealed_at"),
        error_type=_optional_text(row, "error_type"),
        route_key=_optional_text(row, "route_key"),
        output_generation=_integer(row, "output_generation"),
        published_generation=_integer(row, "published_generation"),
    )


def _partition_from_row(row: sqlite3.Row) -> PartitionRecord:
    operation = _optional_text(row, "operation")
    if operation is not None and operation not in ("insert", "update", "delete"):
        raise InvalidTransitionError(f"Durable partition operation '{operation}' is not supported.")
    return PartitionRecord(
        job_id=_text(row, "job_id"),
        partition_id=_text(row, "partition_id"),
        input_ref=_text(row, "input_ref"),
        input_version=_optional_text(row, "input_version"),
        entity_key=_optional_text(row, "entity_key"),
        operation=None if operation is None else cast("ChangeOperation", operation),
        source_revision=_optional_text(row, "source_revision"),
        source_position=_optional_text(row, "source_position"),
        discovered_at=_text(row, "discovered_at"),
    )


def _batch_from_row(row: sqlite3.Row) -> BatchRecord:
    return BatchRecord(
        position=_integer(row, "position"),
        key=WorkUnitKey(
            job_id=_text(row, "job_id"),
            node_id=_text(row, "node_id"),
            partition_id=_text(row, "partition_id"),
        ),
        batch_id=_text(row, "batch_id"),
        output_ref=_text(row, "output_ref"),
        record_count=_integer(row, "record_count"),
        committed_at=_text(row, "committed_at"),
        output_generation=_integer(row, "output_generation"),
    )


class SQLiteDurableStore:
    """Single-host transactional durable execution store.

    The database file must live on a local filesystem that survives
    invocations. Construction validates durability capabilities before creating
    or touching any schema, so an unsupported backend is rejected before any
    durable mutation.
    """

    backend = "sqlite"

    def __init__(
        self,
        path: str | Path,
        *,
        timeout: float = 5.0,
        wal: bool = False,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            raise ValueError("Durable store timeout must be a number.")
        if not math.isfinite(float(timeout)) or not 0 < float(timeout) <= 60:
            raise ValueError("Durable store timeout must be between zero and sixty seconds.")
        if not isinstance(wal, bool):
            raise ValueError("Durable store wal must be a boolean.")

        version = sqlite3.sqlite_version_info
        if wal and not _wal_is_fixed(version):
            raise ValueError(
                "WAL requires a SQLite build with the WAL-reset fix (3.51.3+, or the "
                f"3.50.7+ / 3.44.6+ backports); this build reports {sqlite3.sqlite_version}."
            )

        self.path = Path(path)
        self.timeout = float(timeout)
        self.wal = wal
        self._clock = clock if clock is not None else _utc_now
        self._lock = RLock()
        self._closed = False
        self._connection = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
        self._connection.row_factory = sqlite3.Row

        try:
            self._apply_pragmas()
            self._capabilities = self._probe_capabilities()
            validate_capabilities(self._capabilities, backend=self.backend)
            self._initialize_schema()
        except BaseException:
            self.close()
            raise

    # -- lifecycle ---------------------------------------------------------

    def _apply_pragmas(self) -> None:
        connection = self._connection
        connection.execute(f"PRAGMA journal_mode={'WAL' if self.wal else 'DELETE'}")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(f"PRAGMA busy_timeout={int(self.timeout * 1000)}")

    def _probe_capabilities(self) -> StoreCapabilities:
        """Confirm the durability settings took effect before declaring capabilities."""
        connection = self._connection
        journal_row = connection.execute("PRAGMA journal_mode").fetchone()
        synchronous_row = connection.execute("PRAGMA synchronous").fetchone()
        expected_journal = "wal" if self.wal else "delete"
        durable = (
            journal_row is not None
            and str(journal_row[0]).lower() == expected_journal
            and synchronous_row is not None
            and int(synchronous_row[0]) == 2
        )
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            return StoreCapabilities()
        return StoreCapabilities(
            atomic_conditional_transitions=True,
            generation_validation=True,
            fencing_validation=True,
            durable_reads=durable,
            transactional_publication=True,
        )

    def _initialize_schema(self) -> None:
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_meta'"
            ).fetchone()
            if existing is not None:
                row = connection.execute(
                    "SELECT value FROM schema_meta WHERE key = ?", (_SCHEMA_VERSION_KEY,)
                ).fetchone()
                stored = None if row is None else str(row["value"])
                if stored is not None and stored != str(DURABLE_SCHEMA_VERSION):
                    raise UnsupportedStoreVersionError(
                        f"Durable store schema version '{stored}' is not supported; "
                        f"supported version is {DURABLE_SCHEMA_VERSION}. Incompatible "
                        "stored state is never reinterpreted under a new contract."
                    )
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            connection.execute(
                "INSERT OR IGNORE INTO schema_meta (key, value) VALUES (?, ?)",
                (_SCHEMA_VERSION_KEY, str(DURABLE_SCHEMA_VERSION)),
            )

    @property
    def capabilities(self) -> StoreCapabilities:
        return self._capabilities

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteDurableStore:
        if self._closed:
            raise StoreUnavailableError("Durable store is closed.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _require_connection(self) -> sqlite3.Connection:
        if self._closed:
            raise StoreUnavailableError("Durable store is closed.")
        return self._connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """Run one serializable read-validate-write transaction."""
        with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self._require_connection()

    def _now_text(self) -> str:
        return _format_timestamp(self._clock())

    # -- durable reads -----------------------------------------------------

    def load_job(self, job_id: str) -> JobState | None:
        validate_identifier("job_id", job_id)
        with self._read() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return None if row is None else _job_from_row(row)

    def load_work_unit(self, key: WorkUnitKey) -> WorkUnitState | None:
        with self._read() as connection:
            row = self._work_unit_row(connection, key)
        return None if row is None else _work_unit_from_row(row)

    def load_partition(self, job_id: str, partition_id: str) -> PartitionRecord | None:
        validate_identifier("job_id", job_id)
        validate_identifier("partition_id", partition_id)
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM partitions WHERE job_id = ? AND partition_id = ?",
                (job_id, partition_id),
            ).fetchone()
        return None if row is None else _partition_from_row(row)

    def load_checkpoint(self, pipeline_id: str, lineage: str) -> PipelineCheckpoint | None:
        validate_identifier("pipeline_id", pipeline_id)
        validate_identifier("lineage", lineage)
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM pipeline_checkpoints WHERE pipeline_id = ? AND lineage = ?",
                (pipeline_id, lineage),
            ).fetchone()
        if row is None:
            return None
        return PipelineCheckpoint(
            pipeline_id=_text(row, "pipeline_id"),
            lineage=_text(row, "lineage"),
            completed_position=_optional_text(row, "completed_position"),
            generation=_integer(row, "generation"),
            updated_at=_text(row, "updated_at"),
        )

    def lookup_batch(self, key: WorkUnitKey, batch_id: str) -> BatchRecord | None:
        """Resolve a batch publication whose acknowledgment was lost."""
        validate_identifier("batch_id", batch_id)
        with self._read() as connection:
            row = connection.execute(
                "SELECT b.* FROM batches b JOIN work_units w ON "
                "b.job_id = w.job_id AND b.node_id = w.node_id "
                "AND b.partition_id = w.partition_id "
                "WHERE b.job_id = ? AND b.node_id = ? AND b.partition_id = ? "
                "AND b.batch_id = ? AND b.output_generation = w.output_generation",
                (key.job_id, key.node_id, key.partition_id, batch_id),
            ).fetchone()
        return None if row is None else _batch_from_row(row)

    def work_unit_batches(self, key: WorkUnitKey) -> tuple[BatchRecord, ...]:
        """Return a work unit's committed batches for its *current* output generation.

        Superseded generations stay in the table and stay visible through the
        feed; they are simply no longer this unit's answer.
        """
        with self._read() as connection:
            rows = connection.execute(
                "SELECT b.* FROM batches b JOIN work_units w ON "
                "b.job_id = w.job_id AND b.node_id = w.node_id "
                "AND b.partition_id = w.partition_id "
                "WHERE b.job_id = ? AND b.node_id = ? AND b.partition_id = ? "
                "AND b.output_generation = w.output_generation "
                "ORDER BY b.position",
                (key.job_id, key.node_id, key.partition_id),
            ).fetchall()
        return tuple(_batch_from_row(row) for row in rows)

    def list_committed_batches(
        self, job_id: str, node_id: str, *, after_position: int = 0, limit: int = 100
    ) -> tuple[BatchRecord, ...]:
        """Return committed batches of a node after a position, in commit order.

        Pagination is bounded by ``limit`` and served by the ``batches_feed``
        index, so a consumer never loads the full manifest.
        """
        validate_identifier("job_id", job_id)
        validate_identifier("node_id", node_id)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("list_committed_batches limit must be a positive integer.")
        if not isinstance(after_position, int) or isinstance(after_position, bool):
            raise ValueError("list_committed_batches after_position must be an integer.")
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM batches WHERE job_id = ? AND node_id = ? AND position > ? "
                "ORDER BY position LIMIT ?",
                (job_id, node_id, after_position, limit),
            ).fetchall()
        return tuple(_batch_from_row(row) for row in rows)

    def retention_floor(self, job_id: str) -> int:
        """Return the lowest commit position still guaranteed available."""
        validate_identifier("job_id", job_id)
        with self._read() as connection:
            row = connection.execute(
                "SELECT floor_position FROM retention WHERE job_id = ?", (job_id,)
            ).fetchone()
        return 0 if row is None else _integer(row, "floor_position")

    def set_retention_floor(self, job_id: str, *, floor_position: int) -> int:
        """Declare the lowest commit position still guaranteed available.

        The floor is declarative. Nothing is deleted here, so correctness never
        depends on cleanup having run; a lagging consumer is simply told
        explicitly that its position has expired.
        """
        validate_identifier("job_id", job_id)
        validate_counter("floor_position", floor_position)
        now = self._now_text()
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO retention (job_id, floor_position, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET floor_position = excluded.floor_position, "
                "updated_at = excluded.updated_at",
                (job_id, floor_position, now),
            )
        return floor_position

    def load_consumer_position(
        self, pipeline_id: str, consumer_id: str, job_id: str, node_id: str
    ) -> int | None:
        """Return a consumer's own durable position, or None when it has none."""
        validate_identifier("pipeline_id", pipeline_id)
        validate_identifier("consumer_id", consumer_id)
        validate_identifier("job_id", job_id)
        validate_identifier("node_id", node_id)
        with self._read() as connection:
            row = connection.execute(
                "SELECT position FROM consumer_positions WHERE pipeline_id = ? AND "
                "consumer_id = ? AND job_id = ? AND node_id = ?",
                (pipeline_id, consumer_id, job_id, node_id),
            ).fetchone()
        return None if row is None else _integer(row, "position")

    def acknowledge_consumer_position(
        self,
        pipeline_id: str,
        consumer_id: str,
        job_id: str,
        node_id: str,
        *,
        position: int,
    ) -> int:
        """Advance one consumer's own position.

        This never touches producer progress. A consumer acknowledgment cannot
        roll back a producer commit or affect any other consumer.
        """
        validate_identifier("pipeline_id", pipeline_id)
        validate_identifier("consumer_id", consumer_id)
        validate_identifier("job_id", job_id)
        validate_identifier("node_id", node_id)
        validate_counter("position", position)
        now = self._now_text()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT position FROM consumer_positions WHERE pipeline_id = ? AND "
                "consumer_id = ? AND job_id = ? AND node_id = ?",
                (pipeline_id, consumer_id, job_id, node_id),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO consumer_positions (pipeline_id, consumer_id, job_id, "
                    "node_id, position, generation, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?)",
                    (pipeline_id, consumer_id, job_id, node_id, position, now),
                )
                _fire_failpoint("after_consumer_acknowledged")
                return position
            # A replayed acknowledgment never moves a position backwards.
            advanced = max(_integer(row, "position"), position)
            connection.execute(
                "UPDATE consumer_positions SET position = ?, generation = generation + 1, "
                "updated_at = ? WHERE pipeline_id = ? AND consumer_id = ? AND job_id = ? "
                "AND node_id = ?",
                (advanced, now, pipeline_id, consumer_id, job_id, node_id),
            )
            _fire_failpoint("after_consumer_acknowledged")
            return advanced

    def list_partitions(self, job_id: str) -> tuple[str, ...]:
        """Return every registered input partition id for a job, in stable order."""
        validate_identifier("job_id", job_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT partition_id FROM partitions WHERE job_id = ? ORDER BY partition_id",
                (job_id,),
            ).fetchall()
        return tuple(_text(row, "partition_id") for row in rows)

    def work_unit_partitions(self, job_id: str, node_id: str) -> tuple[str, ...]:
        """Return the partition ids a node has work units for, in stable order."""
        validate_identifier("job_id", job_id)
        validate_identifier("node_id", node_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT partition_id FROM work_units WHERE job_id = ? AND node_id = ? "
                "ORDER BY partition_id",
                (job_id, node_id),
            ).fetchall()
        return tuple(_text(row, "partition_id") for row in rows)

    def unsealed_statuses(self, job_id: str) -> frozenset[str]:
        """Return the distinct statuses of this job's unsealed work units."""
        validate_identifier("job_id", job_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT DISTINCT status FROM work_units WHERE job_id = ? AND status != 'sealed'",
                (job_id,),
            ).fetchall()
        return frozenset(_text(row, "status") for row in rows)

    def node_seals(self, job_id: str) -> tuple[NodeSeal, ...]:
        validate_identifier("job_id", job_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM node_seals WHERE job_id = ? ORDER BY node_id", (job_id,)
            ).fetchall()
        return tuple(
            NodeSeal(
                job_id=_text(row, "job_id"),
                node_id=_text(row, "node_id"),
                final_position=_optional_integer(row, "final_position"),
                sealed_at=_text(row, "sealed_at"),
            )
            for row in rows
        )

    def eligible_work(self, job_id: str, *, limit: int) -> tuple[WorkUnitState, ...]:
        """Return at most ``limit`` dependency-satisfied work units.

        Ordering is by partition then node, so a single worker advances one
        partition through every step before starting the next. That is what
        lets a partition cross a stage boundary while others still lag.

        The query is served by the ``work_units_eligible`` index and stops at
        ``limit``; its cost does not grow with the job's total partition count.
        """
        validate_identifier("job_id", job_id)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("eligible_work limit must be a positive integer.")
        with self._read() as connection:
            rows = connection.execute(ELIGIBLE_WORK_SQL, (job_id, limit)).fetchall()
        return tuple(_work_unit_from_row(row) for row in rows)

    def reclaimable_work(
        self, job_id: str, *, limit: int, include_live: bool = False
    ) -> tuple[WorkUnitState, ...]:
        """Return work units a prior invocation left unfinished.

        Two cases land here, and both are invisible to ``eligible_work``:

        - a process killed mid-unit leaves that unit ``executing`` with a claim
          nobody holds;
        - an attempt that raised leaves its unit ``failed``.

        A resuming invocation must find and reclaim both, or a job could never
        recover from either. This is the caller-triggered invocation retry the
        execution contract describes, distinct from any in-invocation retry
        policy. The result is bounded by how many units can be unfinished at
        once, never by the job's partition count.

        By default only demonstrably dead claims are returned: a failed attempt,
        or an expired or absent lease. ``include_live`` also returns units whose
        lease is still live, which is valid only for a sole worker; with
        concurrent workers it would return work another worker is running.
        """
        validate_identifier("job_id", job_id)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("reclaimable_work limit must be a positive integer.")
        if not isinstance(include_live, bool):
            raise ValueError("include_live must be a boolean.")
        with self._read() as connection:
            if include_live:
                rows = connection.execute(
                    RECLAIMABLE_WORK_EXCLUSIVE_SQL, (job_id, limit)
                ).fetchall()
            else:
                rows = connection.execute(
                    RECLAIMABLE_WORK_SQL, (job_id, self._now_text(), limit)
                ).fetchall()
        return tuple(_work_unit_from_row(row) for row in rows)

    # -- transition helpers ------------------------------------------------

    @staticmethod
    def _work_unit_row(connection: sqlite3.Connection, key: WorkUnitKey) -> sqlite3.Row | None:
        row = connection.execute(
            "SELECT * FROM work_units WHERE job_id = ? AND node_id = ? AND partition_id = ?",
            (key.job_id, key.node_id, key.partition_id),
        ).fetchone()
        return None if row is None else cast("sqlite3.Row", row)

    def _require_work_unit(self, connection: sqlite3.Connection, key: WorkUnitKey) -> sqlite3.Row:
        row = self._work_unit_row(connection, key)
        if row is None:
            raise InvalidTransitionError(
                f"No durable work unit for job='{key.job_id}' node='{key.node_id}' "
                f"partition='{key.partition_id}'."
            )
        return row

    def _require_job_row(self, connection: sqlite3.Connection, job_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise InvalidTransitionError(f"No durable job '{job_id}'.")
        return cast("sqlite3.Row", row)

    def _check_claim(
        self, row: sqlite3.Row, *, owner_id: str, fence_token: int, key: WorkUnitKey
    ) -> None:
        """Reject a writer whose ownership, fence token, or lease is stale."""
        current_owner = _optional_text(row, "owner_id")
        if current_owner is None:
            raise FenceViolationError(
                f"Work unit node='{key.node_id}' partition='{key.partition_id}' holds no "
                f"active claim; writer '{owner_id}' is fenced."
            )
        if current_owner != owner_id:
            raise FenceViolationError(
                f"Work unit node='{key.node_id}' partition='{key.partition_id}' is claimed "
                f"by another owner; writer '{owner_id}' is fenced."
            )
        current_fence = _integer(row, "fence_token")
        if current_fence != fence_token:
            raise FenceViolationError(
                f"Work unit node='{key.node_id}' partition='{key.partition_id}' has fence "
                f"token {current_fence}; writer presented {fence_token}."
            )
        expiry = _optional_text(row, "lease_expires_at")
        if expiry is not None and self._clock() >= _parse_timestamp(expiry):
            raise FenceViolationError(
                f"Lease for node='{key.node_id}' partition='{key.partition_id}' expired at "
                f"{expiry}; writer '{owner_id}' is fenced."
            )

    @staticmethod
    def _check_generation(row: sqlite3.Row, expected: int, *, subject: str) -> int:
        current = _integer(row, "generation")
        if current != expected:
            raise GenerationConflictError(
                f"{subject} is at generation {current}; writer expected {expected}."
            )
        return current

    # -- transitions -------------------------------------------------------

    def initialize_job(
        self,
        job_id: str,
        *,
        pipeline_id: str,
        source_mode: SourceMode,
        declaration_fingerprint: str,
        lineage: str = "default",
        snapshot_id: str | None = None,
        range_start: str | None = None,
        range_end: str | None = None,
    ) -> JobState:
        """Create a job, or return the identical existing one."""
        candidate = JobState(
            job_id=job_id,
            pipeline_id=pipeline_id,
            lineage=lineage,
            source_mode=source_mode,
            declaration_fingerprint=declaration_fingerprint,
            snapshot_id=snapshot_id,
            range_start=range_start,
            range_end=range_end,
        )
        now = self._now_text()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is not None:
                existing = _job_from_row(row)
                conflict = (
                    existing.pipeline_id != candidate.pipeline_id
                    or existing.lineage != candidate.lineage
                    or existing.source_mode != candidate.source_mode
                    or existing.declaration_fingerprint != candidate.declaration_fingerprint
                    or existing.snapshot_id != candidate.snapshot_id
                    or existing.range_start != candidate.range_start
                    or existing.range_end != candidate.range_end
                )
                if conflict:
                    raise InvalidTransitionError(
                        f"Durable job '{job_id}' already exists with a different declaration, "
                        "source identity, or lineage. Stored state is never reinterpreted "
                        "under a new contract; start a new job id instead."
                    )
                return existing
            connection.execute(
                "INSERT INTO jobs (job_id, pipeline_id, lineage, source_mode, snapshot_id, "
                "range_start, range_end, discovery_position, discovery_sealed, status, "
                "declaration_fingerprint, generation, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0, 'open', ?, 0, ?, ?)",
                (
                    job_id,
                    pipeline_id,
                    lineage,
                    source_mode,
                    snapshot_id,
                    range_start,
                    range_end,
                    declaration_fingerprint,
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT OR IGNORE INTO pipeline_checkpoints "
                "(pipeline_id, lineage, completed_position, generation, updated_at) "
                "VALUES (?, ?, NULL, 0, ?)",
                (pipeline_id, lineage, now),
            )
            _fire_failpoint("after_job_insert")
            return _job_from_row(self._require_job_row(connection, job_id))

    def register_discovery(
        self,
        job_id: str,
        *,
        expected_generation: int,
        partitions: Sequence[DiscoveredPartition] = (),
        extra_dependencies: Sequence[DependencyEdge] = (),
        next_position: str | None = None,
        seal: bool = False,
    ) -> JobState:
        """Publish replayable partition inputs and advance discovery together."""
        validate_identifier("job_id", job_id)
        validate_optional_opaque("next_position", next_position)
        if not isinstance(seal, bool):
            raise ValueError("register_discovery seal must be a boolean.")
        now = self._now_text()

        with self._transaction() as connection:
            row = self._require_job_row(connection, job_id)
            job = _job_from_row(row)
            if job.status != "open":
                raise InvalidTransitionError(
                    f"Durable job '{job_id}' is completed; discovery cannot continue."
                )
            if job.discovery_sealed:
                raise InvalidTransitionError(
                    f"Discovery for job '{job_id}' is sealed; no further input may be registered."
                )
            self._check_generation(row, expected_generation, subject=f"Job '{job_id}'")

            for partition in partitions:
                self._insert_partition(connection, job_id, partition, now)
            for partition in partitions:
                for unit in partition.work_units:
                    for upstream in unit.depends_on_nodes:
                        self._insert_dependency(
                            connection,
                            DependencyEdge(
                                node_id=unit.node_id,
                                partition_id=partition.partition_id,
                                depends_on_node_id=upstream,
                                depends_on_partition_id=partition.partition_id,
                            ),
                            job_id=job_id,
                        )
            for edge in extra_dependencies:
                self._insert_dependency(connection, edge, job_id=job_id)

            _fire_failpoint("after_discovery_insert")
            connection.execute(
                "UPDATE jobs SET discovery_position = ?, discovery_sealed = ?, "
                "generation = generation + 1, updated_at = ? WHERE job_id = ?",
                (
                    next_position if next_position is not None else job.discovery_position,
                    1 if seal or job.discovery_sealed else 0,
                    now,
                    job_id,
                ),
            )
            _fire_failpoint("after_discovery_advance")
            return _job_from_row(self._require_job_row(connection, job_id))

    def _insert_partition(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        partition: DiscoveredPartition,
        now: str,
    ) -> None:
        existing = connection.execute(
            "SELECT * FROM partitions WHERE job_id = ? AND partition_id = ?",
            (job_id, partition.partition_id),
        ).fetchone()
        if existing is not None:
            stored = _partition_from_row(existing)
            if (
                stored.input_ref != partition.input_ref
                or stored.input_version != partition.input_version
                or stored.entity_key != partition.entity_key
                or stored.operation != partition.operation
                or stored.source_revision != partition.source_revision
                or stored.source_position != partition.source_position
            ):
                raise InvalidTransitionError(
                    f"Partition '{partition.partition_id}' is already registered for job "
                    f"'{job_id}' with different input identity. Replay must present the "
                    "same replayable input reference."
                )
        else:
            connection.execute(
                "INSERT INTO partitions (job_id, partition_id, input_ref, input_version, "
                "entity_key, operation, source_revision, source_position, discovered_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    partition.partition_id,
                    partition.input_ref,
                    partition.input_version,
                    partition.entity_key,
                    partition.operation,
                    partition.source_revision,
                    partition.source_position,
                    now,
                ),
            )

        for unit in partition.work_units:
            key = WorkUnitKey(
                job_id=job_id, node_id=unit.node_id, partition_id=partition.partition_id
            )
            current = self._work_unit_row(connection, key)
            if current is not None:
                stored_unit = _work_unit_from_row(current)
                if (
                    stored_unit.step_revision != unit.step_revision
                    or stored_unit.params_fingerprint != unit.params_fingerprint
                    or stored_unit.input_scope != unit.input_scope
                ):
                    raise InvalidTransitionError(
                        f"Work unit node='{unit.node_id}' partition='{partition.partition_id}' "
                        "is already registered with a different revision, parameter "
                        "fingerprint, or input scope."
                    )
                continue
            connection.execute(
                "INSERT INTO work_units (job_id, node_id, partition_id, status, input_scope, "
                "step_revision, params_fingerprint, cursor, batch_count, input_exhausted, "
                "pending_deps, owner_id, claim_epoch, fence_token, lease_expires_at, "
                "generation, sealed_at, error_type) "
                "VALUES (?, ?, ?, 'eligible', ?, ?, ?, NULL, 0, 0, 0, NULL, 0, 0, NULL, "
                "0, NULL, NULL)",
                (
                    job_id,
                    unit.node_id,
                    partition.partition_id,
                    unit.input_scope,
                    unit.step_revision,
                    unit.params_fingerprint,
                ),
            )

    def _insert_dependency(
        self, connection: sqlite3.Connection, edge: DependencyEdge, *, job_id: str
    ) -> None:
        """Insert one readiness edge, pre-satisfied when its upstream is already sealed."""
        existing = connection.execute(
            "SELECT satisfied FROM dependencies WHERE job_id = ? AND node_id = ? AND "
            "partition_id = ? AND depends_on_node_id = ? AND depends_on_partition_id = ?",
            (
                job_id,
                edge.node_id,
                edge.partition_id,
                edge.depends_on_node_id,
                edge.depends_on_partition_id,
            ),
        ).fetchone()
        if existing is not None:
            return

        dependent_key = WorkUnitKey(
            job_id=job_id, node_id=edge.node_id, partition_id=edge.partition_id
        )
        upstream_key = WorkUnitKey(
            job_id=job_id,
            node_id=edge.depends_on_node_id,
            partition_id=edge.depends_on_partition_id,
        )
        self._require_work_unit(connection, dependent_key)
        upstream = self._require_work_unit(connection, upstream_key)
        satisfied = 1 if _text(upstream, "status") == "sealed" else 0

        connection.execute(
            "INSERT INTO dependencies (job_id, node_id, partition_id, depends_on_node_id, "
            "depends_on_partition_id, satisfied) VALUES (?, ?, ?, ?, ?, ?)",
            (
                job_id,
                edge.node_id,
                edge.partition_id,
                edge.depends_on_node_id,
                edge.depends_on_partition_id,
                satisfied,
            ),
        )
        if not satisfied:
            connection.execute(
                "UPDATE work_units SET pending_deps = pending_deps + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (job_id, edge.node_id, edge.partition_id),
            )

    def claim_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        expected_generation: int,
        lease_seconds: float | None = None,
        exclusive: bool = False,
    ) -> WorkUnitState:
        """Take fenced ownership of a dependency-satisfied work unit.

        A claim whose lease has expired may always be taken over, and takeover
        raises the fence token so the displaced owner can no longer commit.

        ``exclusive`` additionally permits seizing a claim whose lease is still
        live. It is valid only when the caller guarantees it is the sole worker,
        which a single-worker execution policy does. With concurrent workers it
        would let two live workers hold the same unit, so the execution layer
        passes it only under ``workers == 1``.
        """
        if not isinstance(exclusive, bool):
            raise ValueError("exclusive must be a boolean.")
        validate_identifier("owner_id", owner_id)
        if lease_seconds is not None:
            if not isinstance(lease_seconds, (int, float)) or isinstance(lease_seconds, bool):
                raise ValueError("lease_seconds must be a number or None.")
            if not math.isfinite(float(lease_seconds)) or float(lease_seconds) <= 0:
                raise ValueError("lease_seconds must be positive.")

        now = self._clock()
        expires = (
            None
            if lease_seconds is None
            else _format_timestamp(now + timedelta(seconds=float(lease_seconds)))
        )

        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            status = _text(row, "status")
            if status == "sealed":
                raise WorkUnitUnavailableError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is sealed "
                    "and cannot be claimed."
                )
            if _integer(row, "pending_deps") > 0:
                raise WorkUnitUnavailableError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' has "
                    "unsatisfied dependencies and is not eligible."
                )
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )

            current_owner = _optional_text(row, "owner_id")
            if current_owner is not None and current_owner != owner_id:
                expiry = _optional_text(row, "lease_expires_at")
                live = expiry is None or now < _parse_timestamp(expiry)
                if live and not exclusive:
                    raise FenceViolationError(
                        f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                        f"claimed by a live owner; '{owner_id}' cannot take it over."
                    )

            connection.execute(
                "UPDATE work_units SET status = 'executing', owner_id = ?, "
                "claim_epoch = claim_epoch + 1, fence_token = fence_token + 1, "
                "lease_expires_at = ?, error_type = NULL, generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (owner_id, expires, key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_claim")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def renew_lease(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        lease_seconds: float,
    ) -> WorkUnitState:
        """Extend a live claim's lease as a heartbeat.

        A worker whose heartbeat stalls simply stops renewing, and its lease
        expires so another worker can take the unit over.
        """
        validate_identifier("owner_id", owner_id)
        if not isinstance(lease_seconds, (int, float)) or isinstance(lease_seconds, bool):
            raise ValueError("lease_seconds must be a number.")
        if not math.isfinite(float(lease_seconds)) or float(lease_seconds) <= 0:
            raise ValueError("lease_seconds must be positive.")

        expires = _format_timestamp(self._clock() + timedelta(seconds=float(lease_seconds)))
        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing unit holds a renewable lease."
                )
            connection.execute(
                "UPDATE work_units SET lease_expires_at = ?, generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (expires, key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_lease_renewed")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def release_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
    ) -> WorkUnitState:
        """Give up a claim without failing the unit.

        A cleanly stopped worker releases its claims so other workers proceed
        immediately rather than waiting out a lease.
        """
        validate_identifier("owner_id", owner_id)
        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing unit may be released."
                )
            connection.execute(
                "UPDATE work_units SET status = 'eligible', owner_id = NULL, "
                "lease_expires_at = NULL, generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_unit_released")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def commit_batch(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        batch_id: str,
        output_ref: str,
        next_cursor: str | None = None,
        record_count: int = 0,
        input_exhausted: bool = False,
    ) -> WorkUnitState:
        """Publish an immutable output reference and advance progress together."""
        validate_identifier("owner_id", owner_id)
        validate_identifier("batch_id", batch_id)
        validate_opaque("output_ref", output_ref)
        validate_optional_opaque("next_cursor", next_cursor)
        if not isinstance(record_count, int) or isinstance(record_count, bool) or record_count < 0:
            raise ValueError("record_count must be a non-negative integer.")
        if not isinstance(input_exhausted, bool):
            raise ValueError("input_exhausted must be a boolean.")
        now = self._now_text()

        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)

            # Idempotency is scoped to the unit's current output generation:
            # a recomputed batch legitimately reuses its batch id.
            published = connection.execute(
                "SELECT * FROM batches WHERE job_id = ? AND node_id = ? AND partition_id = ? "
                "AND batch_id = ? AND output_generation = ?",
                (
                    key.job_id,
                    key.node_id,
                    key.partition_id,
                    batch_id,
                    _integer(row, "output_generation"),
                ),
            ).fetchone()
            if published is not None:
                if _text(published, "output_ref") != output_ref:
                    raise InvalidTransitionError(
                        f"Batch '{batch_id}' is already committed for node='{key.node_id}' "
                        f"partition='{key.partition_id}' with a different output reference. "
                        "Committed output is immutable."
                    )
                return _work_unit_from_row(row)

            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing unit may publish a batch."
                )

            connection.execute(
                "INSERT INTO batches (job_id, node_id, partition_id, batch_id, output_ref, "
                "record_count, committed_at, output_generation) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    key.job_id,
                    key.node_id,
                    key.partition_id,
                    batch_id,
                    output_ref,
                    record_count,
                    now,
                    _integer(row, "output_generation"),
                ),
            )
            _fire_failpoint("after_batch_insert")
            connection.execute(
                "UPDATE work_units SET cursor = ?, batch_count = batch_count + 1, "
                "input_exhausted = MAX(input_exhausted, ?), generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (
                    next_cursor if next_cursor is not None else _optional_text(row, "cursor"),
                    1 if input_exhausted else 0,
                    key.job_id,
                    key.node_id,
                    key.partition_id,
                ),
            )
            _fire_failpoint("after_batch_progress")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def mark_input_exhausted(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        next_cursor: str | None = None,
    ) -> WorkUnitState:
        """Record that the unit's selected input scope reached its end.

        End of input is a distinct durable result from an empty batch, so a unit
        whose input is empty can still be sealed without publishing output.
        """
        validate_identifier("owner_id", owner_id)
        validate_optional_opaque("next_cursor", next_cursor)
        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing unit may record end of input."
                )
            connection.execute(
                "UPDATE work_units SET input_exhausted = 1, cursor = ?, "
                "generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (
                    next_cursor if next_cursor is not None else _optional_text(row, "cursor"),
                    key.job_id,
                    key.node_id,
                    key.partition_id,
                ),
            )
            _fire_failpoint("after_input_exhausted")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def seal_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
    ) -> WorkUnitState:
        """Seal a completed partition-step pair and activate its dependents."""
        validate_identifier("owner_id", owner_id)
        now = self._now_text()
        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing unit may be sealed."
                )
            if not _flag(row, "input_exhausted"):
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' has not "
                    "reached end of input; it cannot be sealed."
                )

            connection.execute(
                "UPDATE work_units SET status = 'sealed', sealed_at = ?, owner_id = NULL, "
                "lease_expires_at = NULL, generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (now, key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_unit_sealed")
            connection.execute(
                "UPDATE work_units SET pending_deps = pending_deps - 1, "
                "generation = generation + 1 WHERE job_id = ? AND (node_id, partition_id) IN ("
                "SELECT node_id, partition_id FROM dependencies WHERE job_id = ? AND "
                "depends_on_node_id = ? AND depends_on_partition_id = ? AND satisfied = 0)",
                (key.job_id, key.job_id, key.node_id, key.partition_id),
            )
            connection.execute(
                "UPDATE dependencies SET satisfied = 1 WHERE job_id = ? AND "
                "depends_on_node_id = ? AND depends_on_partition_id = ? AND satisfied = 0",
                (key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_dependencies_activated")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def seal_branch(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        route_key: str,
        partitions: Sequence[DiscoveredPartition] = (),
        dependencies: Sequence[DependencyEdge] = (),
    ) -> WorkUnitState:
        """Commit a routing decision and the work it implies, atomically.

        Routing happens when a partition reaches the branch, because a
        predicate may read values produced by earlier steps. This transaction
        records the chosen route, registers that route's work units and
        readiness edges, and seals the branch unit — all of it, or none.

        Registering edges before sealing is deliberate: the merge's new edge to
        the route's final step is counted while that step is still unsealed, so
        sealing the branch cannot leave the merge spuriously eligible.
        """
        validate_identifier("owner_id", owner_id)
        validate_identifier("route_key", route_key)
        now = self._now_text()

        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Branch node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Branch node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing branch may be routed."
                )
            committed_route = _optional_text(row, "route_key")
            if committed_route is not None and committed_route != route_key:
                raise InvalidTransitionError(
                    f"Branch node='{key.node_id}' partition='{key.partition_id}' is "
                    f"already routed to '{committed_route}'; a committed route is "
                    "authoritative and is never re-decided."
                )

            for partition in partitions:
                self._insert_partition(connection, key.job_id, partition, now)
            for edge in dependencies:
                self._insert_dependency(connection, edge, job_id=key.job_id)
            _fire_failpoint("after_route_registered")

            connection.execute(
                "UPDATE work_units SET status = 'sealed', sealed_at = ?, owner_id = NULL, "
                "lease_expires_at = NULL, route_key = ?, generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (now, route_key, key.job_id, key.node_id, key.partition_id),
            )
            connection.execute(
                "UPDATE work_units SET pending_deps = pending_deps - 1, "
                "generation = generation + 1 WHERE job_id = ? AND (node_id, partition_id) IN ("
                "SELECT node_id, partition_id FROM dependencies WHERE job_id = ? AND "
                "depends_on_node_id = ? AND depends_on_partition_id = ? AND satisfied = 0)",
                (key.job_id, key.job_id, key.node_id, key.partition_id),
            )
            connection.execute(
                "UPDATE dependencies SET satisfied = 1 WHERE job_id = ? AND "
                "depends_on_node_id = ? AND depends_on_partition_id = ? AND satisfied = 0",
                (key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_branch_sealed")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def fail_work_unit(
        self,
        key: WorkUnitKey,
        *,
        owner_id: str,
        fence_token: int,
        expected_generation: int,
        error_type: str,
    ) -> WorkUnitState:
        """Record a failed attempt without activating dependents."""
        validate_identifier("owner_id", owner_id)
        validate_identifier("error_type", error_type)
        with self._transaction() as connection:
            row = self._require_work_unit(connection, key)
            self._check_claim(row, owner_id=owner_id, fence_token=fence_token, key=key)
            self._check_generation(
                row,
                expected_generation,
                subject=f"Work unit node='{key.node_id}' partition='{key.partition_id}'",
            )
            status = _text(row, "status")
            if status != "executing":
                raise InvalidTransitionError(
                    f"Work unit node='{key.node_id}' partition='{key.partition_id}' is "
                    f"'{status}'; only an executing unit may be failed."
                )
            connection.execute(
                "UPDATE work_units SET status = 'failed', owner_id = NULL, "
                "lease_expires_at = NULL, error_type = ?, generation = generation + 1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (error_type, key.job_id, key.node_id, key.partition_id),
            )
            _fire_failpoint("after_unit_failed")
            return _work_unit_from_row(self._require_work_unit(connection, key))

    def mark_published(
        self, key: WorkUnitKey, *, generation: int, files: Sequence[str] | None = None
    ) -> bool:
        """Compare-and-set the materialized generation for a unit.

        Publication progress lives in the authoritative store rather than
        beside the published files. The update applies only while
        ``generation`` is still the unit's current output generation, so a
        publisher that paused across a recomputation loses the race and learns
        about it from the return value: ``False`` means a newer generation
        exists and this publisher's physical writes are stale.
        """
        validate_counter("generation", generation)
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE work_units SET published_generation = ? "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ? "
                "AND output_generation = ?",
                (generation, key.job_id, key.node_id, key.partition_id, generation),
            )
            if cursor.rowcount and files is not None:
                connection.execute(
                    "INSERT OR REPLACE INTO publication_files VALUES (?, ?, ?, ?, ?)",
                    (
                        key.job_id,
                        key.node_id,
                        key.partition_id,
                        generation,
                        json.dumps(sorted(files)),
                    ),
                )
            return bool(cursor.rowcount)

    def publication_snapshot(self, job_id: str) -> dict[str, Any]:
        """Everything a completion manifest must agree with, read atomically.

        Validating a manifest with separate reads can straddle a commit and
        approve a state that never existed. One transaction gives the job's
        identity, status, generation, and the full set of sealed units.
        """
        validate_identifier("job_id", job_id)
        with self._transaction() as connection:
            job_row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if job_row is None:
                return {"job": None, "sealed": {}}
            rows = connection.execute(
                "SELECT node_id, partition_id, output_generation FROM work_units "
                "WHERE job_id = ? AND status = 'sealed'",
                (job_id,),
            ).fetchall()
            file_rows = connection.execute(
                "SELECT p.node_id, p.partition_id, p.files FROM publication_files p "
                "JOIN work_units w ON w.job_id = p.job_id AND w.node_id = p.node_id "
                "AND w.partition_id = p.partition_id "
                "AND w.output_generation = p.output_generation "
                "WHERE p.job_id = ? AND w.status = 'sealed' "
                "AND w.published_generation = w.output_generation",
                (job_id,),
            ).fetchall()
        return {
            "job": _job_from_row(job_row),
            "sealed": {
                (_text(row, "node_id"), _text(row, "partition_id")): _integer(
                    row, "output_generation"
                )
                for row in rows
            },
            "files": {
                (_text(row, "node_id"), _text(row, "partition_id")): json.loads(_text(row, "files"))
                for row in file_rows
            },
        }

    def mark_unpublished(self, key: WorkUnitKey) -> None:
        """Forget that a unit was published, so a rebuild republishes it.

        Used by the explicit rebuild operation, which repairs a published tree
        that was changed behind the store's back.
        """
        with self._transaction() as connection:
            connection.execute(
                "UPDATE work_units SET published_generation = -1 "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (key.job_id, key.node_id, key.partition_id),
            )

    def unpublished_sealed_units(
        self, job_id: str, *, limit: int = 1000
    ) -> tuple[WorkUnitState, ...]:
        """Sealed units whose current output generation is not yet materialized.

        This is how publication recovers: a crash between sealing and writing
        leaves the unit here, and the next invocation picks it up.
        """
        validate_identifier("job_id", job_id)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("unpublished_sealed_units limit must be a positive integer.")
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM work_units WHERE job_id = ? AND status = 'sealed' "
                "AND published_generation != output_generation "
                "ORDER BY partition_id, node_id LIMIT ?",
                (job_id, limit),
            ).fetchall()
        return tuple(_work_unit_from_row(row) for row in rows)

    def invalidate_node(
        self,
        job_id: str,
        node_id: str,
        *,
        partitions: Sequence[str] | None = None,
        invalidated_generations: dict[WorkUnitKey, int] | None = None,
    ) -> tuple[WorkUnitKey, ...]:
        """Invalidate a node's output and everything derived from it.

        This is selective recomputation. It is deliberately not a reset of one
        node: unsealing a node alone would leave its dependents sealed against
        input that no longer exists, and the job would still report complete
        while its manifest described a dataset no single execution produced.

        Every unit in the dependency closure has its ``output_generation``
        bumped, which supersedes its committed output without deleting it, and
        is returned to ``eligible`` with its progress cleared. Edges *into* the
        closure from outside stay satisfied, because that upstream output is
        still current.

        Returns every invalidated work unit key.
        """
        validate_identifier("job_id", job_id)
        validate_identifier("node_id", node_id)
        if partitions is not None:
            for partition_id in partitions:
                validate_identifier("partition_id", partition_id)
        now = self._now_text()

        with self._transaction() as connection:
            job_row = self._require_job_row(connection, job_id)
            seeds = self._invalidation_seeds(connection, job_id, node_id, partitions)
            if not seeds:
                raise InvalidTransitionError(
                    f"No work units to invalidate for node '{node_id}' in job '{job_id}'."
                )
            closure = self._dependency_closure(connection, job_id, seeds)
            self._reject_live_claims(connection, job_id, closure)

            for unit_node_id, unit_partition_id in sorted(closure):
                if invalidated_generations is not None:
                    key = WorkUnitKey(job_id, unit_node_id, unit_partition_id)
                    row = self._require_work_unit(connection, key)
                    invalidated_generations[key] = _integer(row, "output_generation")
                connection.execute(
                    "UPDATE work_units SET status = 'eligible', "
                    "output_generation = output_generation + 1, "
                    "published_generation = -1, "
                    "generation = generation + 1, cursor = NULL, batch_count = 0, "
                    "input_exhausted = 0, owner_id = NULL, lease_expires_at = NULL, "
                    "sealed_at = NULL, error_type = NULL, route_key = NULL "
                    "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                    (job_id, unit_node_id, unit_partition_id),
                )
                # An edge whose upstream is also invalidated must wait again.
                # One whose upstream is outside the closure stays satisfied.
                for dep_node_id, dep_partition_id in self._satisfied_upstreams(
                    connection, job_id, unit_node_id, unit_partition_id
                ):
                    if (dep_node_id, dep_partition_id) not in closure:
                        continue
                    connection.execute(
                        "UPDATE dependencies SET satisfied = 0 WHERE job_id = ? AND "
                        "node_id = ? AND partition_id = ? AND depends_on_node_id = ? "
                        "AND depends_on_partition_id = ?",
                        (
                            job_id,
                            unit_node_id,
                            unit_partition_id,
                            dep_node_id,
                            dep_partition_id,
                        ),
                    )
                    connection.execute(
                        "UPDATE work_units SET pending_deps = pending_deps + 1 "
                        "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                        (job_id, unit_node_id, unit_partition_id),
                    )

            # A seal would publish a final manifest position for output that is
            # no longer current.
            for invalidated_node in sorted({node for node, _ in closure}):
                connection.execute(
                    "DELETE FROM node_seals WHERE job_id = ? AND node_id = ?",
                    (job_id, invalidated_node),
                )
            _fire_failpoint("after_invalidation")

            if _text(job_row, "status") != "open":
                # The lineage checkpoint is deliberately left alone: the range
                # was genuinely completed, and this produces a new generation
                # of it rather than un-completing it.
                connection.execute(
                    "UPDATE jobs SET status = 'open', generation = generation + 1, "
                    "updated_at = ? WHERE job_id = ?",
                    (now, job_id),
                )
            _fire_failpoint("after_job_reopened")

        return tuple(
            WorkUnitKey(job_id=job_id, node_id=node, partition_id=partition)
            for node, partition in sorted(closure)
        )

    @staticmethod
    def _invalidation_seeds(
        connection: sqlite3.Connection,
        job_id: str,
        node_id: str,
        partitions: Sequence[str] | None,
    ) -> set[tuple[str, str]]:
        if partitions is None:
            rows = connection.execute(
                "SELECT partition_id FROM work_units WHERE job_id = ? AND node_id = ?",
                (job_id, node_id),
            ).fetchall()
            return {(node_id, _text(row, "partition_id")) for row in rows}
        found: set[tuple[str, str]] = set()
        for partition_id in partitions:
            row = connection.execute(
                "SELECT 1 FROM work_units WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (job_id, node_id, partition_id),
            ).fetchone()
            if row is None:
                raise InvalidTransitionError(
                    f"No work unit for node='{node_id}' partition='{partition_id}'."
                )
            found.add((node_id, partition_id))
        return found

    @staticmethod
    def _dependency_closure(
        connection: sqlite3.Connection, job_id: str, seeds: set[tuple[str, str]]
    ) -> set[tuple[str, str]]:
        """Walk dependents iteratively, bounded by the job's work unit count.

        A declaration is a DAG, but the walk must still terminate on corrupt
        durable state, so it carries a visited set and a hard bound.
        """
        total = connection.execute(
            "SELECT COUNT(*) AS total FROM work_units WHERE job_id = ?", (job_id,)
        ).fetchone()
        bound = 1 if total is None else max(1, _integer(total, "total"))

        closure = set(seeds)
        frontier = set(seeds)
        for _ in range(bound):
            if not frontier:
                break
            discovered: set[tuple[str, str]] = set()
            for node_id, partition_id in frontier:
                rows = connection.execute(
                    "SELECT node_id, partition_id FROM dependencies WHERE job_id = ? "
                    "AND depends_on_node_id = ? AND depends_on_partition_id = ?",
                    (job_id, node_id, partition_id),
                ).fetchall()
                for row in rows:
                    candidate = (_text(row, "node_id"), _text(row, "partition_id"))
                    if candidate not in closure:
                        discovered.add(candidate)
            closure |= discovered
            frontier = discovered
        return closure

    @staticmethod
    def _satisfied_upstreams(
        connection: sqlite3.Connection, job_id: str, node_id: str, partition_id: str
    ) -> list[tuple[str, str]]:
        rows = connection.execute(
            "SELECT depends_on_node_id, depends_on_partition_id FROM dependencies "
            "WHERE job_id = ? AND node_id = ? AND partition_id = ? AND satisfied = 1",
            (job_id, node_id, partition_id),
        ).fetchall()
        return [
            (_text(row, "depends_on_node_id"), _text(row, "depends_on_partition_id"))
            for row in rows
        ]

    def _reject_live_claims(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        closure: set[tuple[str, str]],
    ) -> None:
        """Fail closed rather than fencing a running worker mid-unit."""
        now = self._clock()
        for node_id, partition_id in sorted(closure):
            row = connection.execute(
                "SELECT owner_id, lease_expires_at, status FROM work_units "
                "WHERE job_id = ? AND node_id = ? AND partition_id = ?",
                (job_id, node_id, partition_id),
            ).fetchone()
            if row is None or _optional_text(row, "owner_id") is None:
                continue
            expiry = _optional_text(row, "lease_expires_at")
            if expiry is None or now < _parse_timestamp(expiry):
                raise InvalidTransitionError(
                    f"Work unit node='{node_id}' partition='{partition_id}' is claimed "
                    f"by a live worker; invalidation would discard work already in "
                    "flight. Wait for it to finish or for its lease to expire."
                )

    def complete_job(
        self,
        job_id: str,
        *,
        expected_generation: int,
        checkpoint_position: str | None = None,
    ) -> JobState:
        """Seal node manifests, publish job completion, and advance the lineage checkpoint."""
        validate_identifier("job_id", job_id)
        validate_optional_opaque("checkpoint_position", checkpoint_position)
        now = self._now_text()

        with self._transaction() as connection:
            row = self._require_job_row(connection, job_id)
            job = _job_from_row(row)
            if job.status == "completed":
                raise InvalidTransitionError(
                    f"Durable job '{job_id}' is already completed; reload authoritative "
                    "state instead of completing it twice."
                )
            self._check_generation(row, expected_generation, subject=f"Job '{job_id}'")
            if not job.discovery_sealed:
                raise InvalidTransitionError(
                    f"Discovery for job '{job_id}' is not sealed; production is still open."
                )
            if checkpoint_position is not None and job.source_mode != "incremental":
                raise ValueError(
                    "checkpoint_position may only advance the lineage checkpoint of an "
                    "incremental job."
                )

            outstanding = connection.execute(
                "SELECT COUNT(*) AS total FROM work_units WHERE job_id = ? AND status != 'sealed'",
                (job_id,),
            ).fetchone()
            if outstanding is not None and _integer(outstanding, "total") > 0:
                raise InvalidTransitionError(
                    f"Durable job '{job_id}' has "
                    f"{_integer(outstanding, 'total')} unsealed work units; required work "
                    "must complete before job completion."
                )

            node_rows = connection.execute(
                "SELECT DISTINCT node_id FROM work_units WHERE job_id = ? ORDER BY node_id",
                (job_id,),
            ).fetchall()
            for node_row in node_rows:
                node_id = _text(node_row, "node_id")
                final = connection.execute(
                    "SELECT MAX(position) AS final_position FROM batches "
                    "WHERE job_id = ? AND node_id = ?",
                    (job_id, node_id),
                ).fetchone()
                connection.execute(
                    "INSERT OR REPLACE INTO node_seals (job_id, node_id, final_position, "
                    "sealed_at) VALUES (?, ?, ?, ?)",
                    (
                        job_id,
                        node_id,
                        None if final is None else _optional_integer(final, "final_position"),
                        now,
                    ),
                )
            _fire_failpoint("after_node_seals")

            connection.execute(
                "UPDATE jobs SET status = 'completed', generation = generation + 1, "
                "updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            _fire_failpoint("after_job_completed")

            if checkpoint_position is not None:
                connection.execute(
                    "UPDATE pipeline_checkpoints SET completed_position = ?, "
                    "generation = generation + 1, updated_at = ? "
                    "WHERE pipeline_id = ? AND lineage = ?",
                    (checkpoint_position, now, job.pipeline_id, job.lineage),
                )
                _fire_failpoint("after_checkpoint_advanced")

            return _job_from_row(self._require_job_row(connection, job_id))


__all__ = [
    "ELIGIBLE_WORK_SQL",
    "RECLAIMABLE_WORK_EXCLUSIVE_SQL",
    "RECLAIMABLE_WORK_SQL",
    "SQLiteDurableStore",
]
