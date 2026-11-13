"""One retry policy and one execution mechanism, reusable at any boundary.

The same :class:`RetryPolicy` type and semantics apply whether a policy is
attached declaratively to a step or invoked through the :func:`retry` callable
around a batch, a record, or a single operation. Developers learn one contract.

A retry repeats the scope it is attached to. Nested boundaries acquire no policy
automatically: attaching one is always explicit, because nested attempts
multiply.

Retries do not provide exactly-once effects. Every retried scope must be
replay-safe: a timeout can occur after an external write already succeeded.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

T = TypeVar("T")

DEFAULT_MAX_RETRIES = 3
MAX_BACKOFF_SECONDS = 30.0
BASE_BACKOFF_SECONDS = 1.0


class RetryError(Exception):
    """Base class for retry-mechanism failures."""


class RetryPolicyError(RetryError):
    """Raised when a user-supplied predicate itself fails.

    A predicate failure is a policy error, never a retryable failure.
    """


class RetryBudgetExhaustedError(RetryError):
    """Raised when a shared retry budget has no attempts left."""


class RetryCancelled(RetryError):
    """Raised when a retry is abandoned because its deadline or cancellation hit."""


@dataclass(frozen=True)
class FailureContext:
    """Structured facts about one failed attempt, supplied to predicates."""

    exception: BaseException | None
    attempt: int
    scope: str
    elapsed_seconds: float
    result: Any = None
    from_result: bool = False
    """True when a result classifier flagged a returned value rather than an
    exception being raised."""

    @property
    def error_code(self) -> str | None:
        """An explicit error code when the exception exposes one."""
        for attribute in ("code", "status", "status_code"):
            value = getattr(self.exception, attribute, None)
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return str(value)
        return None


# Transient by default: connection failures, timeouts, and temporary service
# failures. Arbitrary exception text is never classified as transient.
_TRANSIENT_TYPES: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    BrokenPipeError,
)

# Permanent classifications take precedence over broad transport categories.
_PERMANENT_TYPES: tuple[type[BaseException], ...] = (
    TypeError,
    ValueError,
    KeyError,
    IndexError,
    AttributeError,
    NotImplementedError,
    PermissionError,
    KeyboardInterrupt,
    SystemExit,
    MemoryError,
)

_RATE_LIMIT_CODES = frozenset({"429", "503", "502", "504"})


def default_classifier(failure: FailureContext) -> bool:
    """Classify recognized transient failures, excluding permanent ones.

    Excluded by design: invalid input, authentication and authorization
    failures, programming errors, cancellation, and process termination.
    """
    if failure.from_result:
        # The caller explicitly opted into result classification, so a flagged
        # result is retryable unless a custom condition says otherwise.
        return True
    error = failure.exception
    if error is None:
        return False
    if isinstance(error, _PERMANENT_TYPES):
        return False
    if isinstance(error, _TRANSIENT_TYPES):
        return True
    return failure.error_code in _RATE_LIMIT_CODES


@dataclass
class RetryBudget:
    """A retry allowance shared across nested boundaries.

    Exhausting the budget prevents an enclosing policy from restarting the
    exhausted work.
    """

    remaining: int | None = None
    deadline_seconds: float | None = None
    shutdown_margin_seconds: float = 0.0
    _started: float = field(default_factory=time.monotonic, repr=False)

    def __post_init__(self) -> None:
        if self.remaining is not None:
            if not isinstance(self.remaining, int) or isinstance(self.remaining, bool):
                raise TypeError("RetryBudget.remaining must be an integer or None.")
            if self.remaining < 0:
                raise ValueError("RetryBudget.remaining must not be negative.")

    @property
    def remaining_seconds(self) -> float:
        if self.deadline_seconds is None:
            return float("inf")
        return self.deadline_seconds - (time.monotonic() - self._started)

    def fits(self, delay: float) -> bool:
        """Whether another attempt can start after waiting ``delay``."""
        return self.remaining_seconds > delay + self.shutdown_margin_seconds

    def take(self) -> None:
        """Consume one shared retry attempt."""
        if self.remaining is None:
            return
        if self.remaining <= 0:
            raise RetryBudgetExhaustedError(
                "The shared retry budget is exhausted; no further attempts may start."
            )
        self.remaining -= 1


@dataclass(frozen=True)
class RetryPolicy:
    """One retry contract, usable at any selected boundary.

    ``when`` replaces or extends the default classifier. ``on_result`` is off by
    default, so a successful return is accepted unless a caller explicitly opts
    in for an API that returns failures instead of raising.
    """

    max_retries: int = DEFAULT_MAX_RETRIES
    when: Callable[[FailureContext], bool] = default_classifier
    on_result: Callable[[Any], bool] | None = None
    max_elapsed_seconds: float | None = None
    base_seconds: float = BASE_BACKOFF_SECONDS
    max_backoff_seconds: float = MAX_BACKOFF_SECONDS
    jitter: Callable[[float, float], float] = random.uniform
    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic

    def __post_init__(self) -> None:
        if not isinstance(self.max_retries, int) or isinstance(self.max_retries, bool):
            raise TypeError("RetryPolicy.max_retries must be an integer.")
        if self.max_retries < 0:
            raise ValueError("RetryPolicy.max_retries must not be negative.")
        if not callable(self.when):
            raise TypeError("RetryPolicy.when must be callable.")
        if self.on_result is not None and not callable(self.on_result):
            raise TypeError("RetryPolicy.on_result must be callable or None.")
        for name in ("base_seconds", "max_backoff_seconds"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
                raise ValueError(f"RetryPolicy.{name} must be a non-negative number.")

    @property
    def max_attempts(self) -> int:
        """Total attempts allowed, including the initial one."""
        return self.max_retries + 1

    def backoff_seconds(self, retry_number: int) -> float:
        """Full-jitter exponential backoff before retry ``retry_number`` (from 1).

        Sampled uniformly between zero and ``min(max_backoff, base * 2^(n-1))``.
        """
        if retry_number < 1:
            raise ValueError("retry_number starts at 1.")
        ceiling = min(self.max_backoff_seconds, self.base_seconds * (2 ** (retry_number - 1)))
        return self.jitter(0.0, ceiling)

    def should_retry(self, failure: FailureContext) -> bool:
        """Apply this policy's condition, surfacing predicate errors as policy errors."""
        try:
            return bool(self.when(failure))
        except Exception as error:
            raise RetryPolicyError(
                f"Retry predicate for scope '{failure.scope}' raised "
                f"{type(error).__name__}; predicate failures are policy errors."
            ) from error


def retry(
    operation: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    scope: str = "operation",
    budget: RetryBudget | None = None,
) -> T:
    """Execute ``operation`` under ``policy``, repeating only that scope.

    Cancellation and remaining time are checked before waiting and before each
    new attempt. An attempt starts only when the delay plus the shutdown margin
    fits the remaining budget.
    """
    active = policy if policy is not None else RetryPolicy()
    started = active.monotonic()
    attempt = 0

    while True:
        attempt += 1
        failure: FailureContext | None = None
        try:
            result = operation()
        except RetryError:
            raise
        except BaseException as error:  # noqa: BLE001 - classification decides
            failure = FailureContext(
                exception=error,
                attempt=attempt,
                scope=scope,
                elapsed_seconds=active.monotonic() - started,
            )
        else:
            if active.on_result is None:
                return result
            try:
                failed = bool(active.on_result(result))
            except Exception as error:
                raise RetryPolicyError(
                    f"Retry result classifier for scope '{scope}' raised "
                    f"{type(error).__name__}; predicate failures are policy errors."
                ) from error
            if not failed:
                return result
            failure = FailureContext(
                exception=None,
                attempt=attempt,
                scope=scope,
                elapsed_seconds=active.monotonic() - started,
                result=result,
                from_result=True,
            )

        if attempt >= active.max_attempts or not active.should_retry(failure):
            _surface(failure)

        elapsed = active.monotonic() - started
        if active.max_elapsed_seconds is not None and elapsed >= active.max_elapsed_seconds:
            _surface(failure)

        delay = active.backoff_seconds(attempt)
        if budget is not None:
            if not budget.fits(delay):
                raise RetryCancelled(
                    f"Scope '{scope}' has insufficient remaining time for another "
                    "attempt; it stopped at a durable boundary instead."
                )
            budget.take()
        active.sleep(delay)


def _surface(failure: FailureContext) -> Any:
    """Raise the final failure, preserving its attempt context."""
    if failure.exception is not None:
        raise failure.exception
    raise RetryError(
        f"Scope '{failure.scope}' returned a failure result after {failure.attempt} attempts."
    )


__all__ = [
    "BASE_BACKOFF_SECONDS",
    "DEFAULT_MAX_RETRIES",
    "MAX_BACKOFF_SECONDS",
    "FailureContext",
    "RetryBudget",
    "RetryBudgetExhaustedError",
    "RetryCancelled",
    "RetryError",
    "RetryPolicy",
    "RetryPolicyError",
    "default_classifier",
    "retry",
]
