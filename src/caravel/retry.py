"""One retry configuration class and one retry loop, reusable at any boundary.

:class:`RetryOptions` is the only retry configuration surface. The same instance
is accepted as ``retry=`` by ``Stage``, ``Step``, and
:func:`retry_operation`, and every one of them runs the same
loop. There are no dictionaries, keyword bundles, decorators, or
wrapper-specific retry APIs.

Retry is always explicit. A boundary with ``retry=None`` does not retry, and an
enabled boundary does not implicitly enable its nested boundaries, because
nested attempts multiply. ``RetryOptions()`` selects the documented defaults.

Inside managed execution a private control is installed in a ``ContextVar``, so
a :func:`retry_operation` call written inside a callback inherits the
invocation's deadline, cancellation state, ownership facts, and shared retry
allowance without the caller passing a context argument. Outside the runner the
same function runs with ordinary sleeping and its own local limits, and claims
no durable recovery.

Retries do not provide exactly-once effects. Every retried scope must be
replay-safe: a timeout can occur after an external write already succeeded.
"""

from __future__ import annotations

import math
import random
import time
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar

T = TypeVar("T")

DEFAULT_MAX_ATTEMPTS = 4
MAX_RETRY_DELAY_SECONDS = 16
DEFAULT_INVOCATION_RETRY_ALLOWANCE = 100


class RetryError(Exception):
    """Base class for retry-mechanism failures."""


class RetryPolicyError(RetryError):
    """Raised when a user-supplied predicate or delay function itself fails.

    A predicate failure is a policy error, never a retryable failure.
    """


class RetryResultExhaustedError(RetryError):
    """The final returned result was rejected by the result predicate."""


class RetryBudgetExhaustedError(RetryError):
    """Raised when the invocation's shared retry allowance has nothing left."""


class RetryCancelledError(RetryError):
    """Raised when a retry is abandoned because its deadline or cancellation hit."""


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


def _error_code(error: BaseException) -> str | None:
    """An explicit error code when the exception exposes one."""
    for attribute in ("code", "status", "status_code"):
        value = getattr(error, attribute, None)
        if isinstance(value, (str, int)) and not isinstance(value, bool):
            return str(value)
    return None


def _default_should_retry_error(error: BaseException) -> bool:
    """Classify recognized transient failures, excluding permanent ones.

    Excluded by design: invalid input, authentication and authorization
    failures, programming errors, cancellation, and process termination.
    """
    if isinstance(error, _PERMANENT_TYPES):
        return False
    if isinstance(error, _TRANSIENT_TYPES):
        return True
    return _error_code(error) in _RATE_LIMIT_CODES


def _default_should_retry_result(result: Any) -> bool:
    """Accept any returned value; result-based retry is opt-in."""
    del result
    return False


def compute_retry_delay(attempt: int) -> float:
    """Capped exponential delay plus up to one second of jitter."""
    if type(attempt) is not int or attempt < 1:
        raise ValueError("attempt must be a positive integer.")
    # Cap the exponent too, avoiding huge integers for large attempt counts.
    exponential = min(2 ** min(attempt - 1, 4), MAX_RETRY_DELAY_SECONDS)
    return float(exponential) + random.uniform(0, 1)


@dataclass(frozen=True)
class RetryOptions:
    """One typed retry contract, usable at every supported boundary.

    ``should_retry_error`` receives the raised exception. ``should_retry_result``
    receives the returned value and defaults to accepting it, so a successful
    return is accepted unless a caller explicitly opts in for an API that
    reports failure by returning rather than raising. ``max_attempts`` counts
    the initial call, so ``max_attempts=1`` permits that call without retry.
    ``delay_seconds`` receives the completed attempt number and returns the
    wait before the next one.
    """

    should_retry_error: Callable[[BaseException], bool] = _default_should_retry_error
    should_retry_result: Callable[[Any], bool] = _default_should_retry_result
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    delay_seconds: Callable[[int], float] = compute_retry_delay

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or isinstance(self.max_attempts, bool):
            raise TypeError(
                f"RetryOptions.max_attempts must be an integer, got "
                f"{type(self.max_attempts).__name__}."
            )
        if self.max_attempts < 1:
            raise ValueError(
                "RetryOptions.max_attempts counts the initial call and must be at least 1."
            )
        for name in ("should_retry_error", "should_retry_result", "delay_seconds"):
            if not callable(getattr(self, name)):
                raise TypeError(
                    f"RetryOptions.{name} must be callable, got "
                    f"{type(getattr(self, name)).__name__}."
                )


class _RetryControl(Protocol):
    """The private hook managed execution installs around user callbacks."""

    def before_attempt(self, attempt: int) -> None:
        """Check deadline/cancellation/ownership and charge shared retries."""
        ...

    def wait(self, seconds: float) -> None:
        """Wait within the active budget, or raise before it expires."""
        ...


_active_retry_control: ContextVar[_RetryControl | None] = ContextVar(
    "caravel_retry_control", default=None
)


def retry_operation(
    *,
    operation: Callable[[], T],
    retry: RetryOptions | None = None,
) -> tuple[T, int]:
    """Run a callable until accepted; return its result and total attempts.

    Only ``operation`` repeats. Surrounding work in the enclosing callback is
    repeated only when an explicitly enabled outer boundary later restarts it.
    Inside managed execution the invocation's deadline, cancellation, and shared
    retry allowance apply automatically; outside it, ordinary sleeping and these
    local limits apply and nothing durable is claimed.
    """
    if retry is not None and not isinstance(retry, RetryOptions):
        raise TypeError(
            f"retry_operation.retry must be a RetryOptions or None, got {type(retry).__name__}."
        )
    if not callable(operation):
        raise TypeError(f"operation must be callable, got {type(operation).__name__}.")
    options = retry if retry is not None else RetryOptions()
    control = _active_retry_control.get()

    for attempt in range(1, options.max_attempts + 1):
        if control is not None:
            control.before_attempt(attempt)

        try:
            result = operation()
        except RetryError:
            # Budget exhaustion, cancellation, and policy failures propagate
            # through outer retries rather than being reclassified by users.
            raise
        except Exception as error:
            try:
                should_retry = options.should_retry_error(error)
            except Exception as predicate_error:
                raise RetryPolicyError("Error predicate failed.") from predicate_error
            if not should_retry or attempt == options.max_attempts:
                raise
        else:
            try:
                should_retry = options.should_retry_result(result)
            except Exception as predicate_error:
                raise RetryPolicyError("Result predicate failed.") from predicate_error
            if not should_retry:
                return result, attempt
            if attempt == options.max_attempts:
                raise RetryResultExhaustedError(f"Result rejected after {attempt} attempts.")

        try:
            delay = options.delay_seconds(attempt)
        except Exception as delay_error:
            raise RetryPolicyError("Retry delay calculation failed.") from delay_error
        if (
            isinstance(delay, bool)
            or not isinstance(delay, (int, float))
            or not math.isfinite(delay)
            or delay < 0
        ):
            raise RetryPolicyError("Retry delay must be finite and nonnegative.")
        if control is None:
            time.sleep(delay)
        else:
            control.wait(delay)

    raise AssertionError("Retry operation completed without a result.")


__all__ = [
    "DEFAULT_INVOCATION_RETRY_ALLOWANCE",
    "DEFAULT_MAX_ATTEMPTS",
    "MAX_RETRY_DELAY_SECONDS",
    "RetryBudgetExhaustedError",
    "RetryCancelledError",
    "RetryError",
    "RetryOptions",
    "RetryPolicyError",
    "RetryResultExhaustedError",
    "compute_retry_delay",
    "retry_operation",
]
