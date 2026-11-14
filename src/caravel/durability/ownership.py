"""Single-executor ownership of a recovery database.

One executor runs a recovery database at a time. Ownership is a transaction
held open on a sibling file for the executor's lifetime, so the operating
system answers the liveness question: the lock lasts exactly as long as the
process holding it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .types import DurabilityError

OWNER_FILENAME = "owner.sqlite3"


class ExecutorBusyError(DurabilityError):
    """Raised when another executor already owns the recovery database."""


class ExecutorOwnership:
    """Exclusive ownership of one recovery database.

    The ownership connection is separate from the connection running the job,
    so holding ownership leaves the run's own transactions free.
    """

    def __init__(self, directory: Path | str) -> None:
        self.path = Path(directory) / OWNER_FILENAME
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: sqlite3.Connection | None = None

    def acquire(self, *, database: Path | str) -> None:
        connection = sqlite3.connect(self.path, timeout=0, isolation_level=None)
        try:
            connection.execute("PRAGMA busy_timeout=0")
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            connection.close()
            raise ExecutorBusyError(
                f"Another executor is running {database}. One executor runs a "
                "recovery database at a time; wait for it to finish."
            ) from error
        self._connection = connection

    def release(self) -> None:
        connection, self._connection = self._connection, None
        if connection is None:
            return
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        connection.close()

    @property
    def held(self) -> bool:
        return self._connection is not None

    def __enter__(self) -> ExecutorOwnership:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


__all__ = ["OWNER_FILENAME", "ExecutorBusyError", "ExecutorOwnership"]
