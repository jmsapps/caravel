"""Fenced leases for concurrent workers.

Concurrent workers claim eligible partition-step pairs through the same
authoritative transaction every other transition uses. Three rules make that
safe, and all three are enforced by the store rather than by convention:

- a claim is atomic, so exactly one of two racing workers wins;
- a claim carries an increasing fence token, so a displaced worker's writes are
  rejected even if it is still running;
- a lease expires, so a worker that dies or stalls cannot hold a unit forever.

A Caravel lease cannot retract an external write. Destination writes still
require idempotency or destination-enforced version checks.
"""

from __future__ import annotations

from dataclasses import dataclass

from .sqlite import SQLiteDurableStore
from .types import (
    DurabilityError,
    FenceViolationError,
    WorkUnitKey,
    WorkUnitState,
    validate_identifier,
)

DEFAULT_TTL_SECONDS = 60.0
DEFAULT_HEARTBEAT_SECONDS = 10.0


class ConcurrencyUnsupportedError(DurabilityError):
    """Raised when a backend cannot support the requested concurrency."""


@dataclass(frozen=True)
class LeasePolicy:
    """How long a claim survives and how often its holder renews it."""

    ttl_seconds: float = DEFAULT_TTL_SECONDS
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS

    def __post_init__(self) -> None:
        for name in ("ttl_seconds", "heartbeat_seconds"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"LeasePolicy.{name} must be a number.")
            if value <= 0:
                raise ValueError(f"LeasePolicy.{name} must be positive.")
        if self.heartbeat_seconds >= self.ttl_seconds:
            raise ValueError(
                "LeasePolicy.heartbeat_seconds must be shorter than ttl_seconds, or a "
                "lease would expire before its holder could renew it."
            )


def validate_concurrency(store: SQLiteDurableStore, *, workers: int) -> None:
    """Reject concurrent execution on a backend that cannot support it.

    Validation happens before any durable mutation, so an unsupported backend
    never partially executes a job.
    """
    if workers < 1:
        raise ValueError("workers must be at least 1.")
    if workers == 1:
        return
    capabilities = store.capabilities
    missing = capabilities.missing()
    if missing:
        raise ConcurrencyUnsupportedError(
            f"Backend '{getattr(store, 'backend', type(store).__name__)}' cannot support "
            f"{workers} workers; it lacks: {', '.join(missing)}. Concurrent workers "
            "require atomic fenced claims."
        )


class LeaseHolder:
    """One worker's fenced claim on one work unit."""

    def __init__(
        self,
        store: SQLiteDurableStore,
        key: WorkUnitKey,
        *,
        owner_id: str,
        policy: LeasePolicy,
        state: WorkUnitState,
    ) -> None:
        self._store = store
        self.key = key
        self.owner_id = validate_identifier("owner_id", owner_id)
        self.policy = policy
        self._state = state
        self._released = False

    @property
    def state(self) -> WorkUnitState:
        return self._state

    @property
    def fence_token(self) -> int:
        return self._state.fence_token

    def observe(self, state: WorkUnitState) -> None:
        """Record authoritative state returned by another transition."""
        self._state = state

    def heartbeat(self) -> WorkUnitState:
        """Renew this claim's lease. Stops being possible once fenced out."""
        if self._released:
            raise FenceViolationError(
                f"Lease on node='{self.key.node_id}' partition='{self.key.partition_id}' "
                "was already released."
            )
        self._state = self._store.renew_lease(
            self.key,
            owner_id=self.owner_id,
            fence_token=self._state.fence_token,
            expected_generation=self._state.generation,
            lease_seconds=self.policy.ttl_seconds,
        )
        return self._state

    def release(self) -> WorkUnitState:
        """Give the claim back so another worker need not wait out the lease."""
        if self._released:
            return self._state
        self._state = self._store.release_work_unit(
            self.key,
            owner_id=self.owner_id,
            fence_token=self._state.fence_token,
            expected_generation=self._state.generation,
        )
        self._released = True
        return self._state


def acquire(
    store: SQLiteDurableStore,
    key: WorkUnitKey,
    *,
    owner_id: str,
    expected_generation: int,
    policy: LeasePolicy,
    exclusive: bool = False,
) -> LeaseHolder:
    """Claim one work unit under a fenced lease."""
    state = store.claim_work_unit(
        key,
        owner_id=owner_id,
        expected_generation=expected_generation,
        lease_seconds=policy.ttl_seconds,
        exclusive=exclusive,
    )
    return LeaseHolder(store, key, owner_id=owner_id, policy=policy, state=state)


__all__ = [
    "DEFAULT_HEARTBEAT_SECONDS",
    "DEFAULT_TTL_SECONDS",
    "ConcurrencyUnsupportedError",
    "LeaseHolder",
    "LeasePolicy",
    "acquire",
    "validate_concurrency",
]
