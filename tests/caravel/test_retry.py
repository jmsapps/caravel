"""One retry contract at step, batch, record, and operation boundaries."""

from __future__ import annotations

from typing import Any

import pytest

from caravel.retry import (
    BASE_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    FailureContext,
    RetryBudget,
    RetryBudgetExhaustedError,
    RetryCancelled,
    RetryError,
    RetryPolicy,
    RetryPolicyError,
    default_classifier,
    retry,
)


class _Clock:
    """A controlled monotonic clock that advances only when slept on."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def _policy(clock: _Clock, **kwargs: Any) -> RetryPolicy:
    """A policy with deterministic time and maximum jitter."""
    defaults: dict[str, Any] = {
        "jitter": lambda low, high: high,
        "sleep": clock.sleep,
        "monotonic": clock.monotonic,
    }
    defaults.update(kwargs)
    return RetryPolicy(**defaults)


class _Vendor(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


# ---------------------------------------------------------------------------
# Defaults (coverage 1 and 2)
# ---------------------------------------------------------------------------


def test_the_default_policy_allows_four_total_attempts() -> None:
    clock = _Clock()
    attempts = {"n": 0}

    def always_fails() -> None:
        attempts["n"] += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        retry(always_fails, policy=_policy(clock))

    assert attempts["n"] == 4
    assert len(clock.slept) == 3


def test_backoff_is_full_jitter_exponential_capped_at_thirty_seconds() -> None:
    clock = _Clock()
    sampled: list[tuple[float, float]] = []

    def record_jitter(low: float, high: float) -> float:
        sampled.append((low, high))
        return 0.0

    policy = RetryPolicy(
        max_retries=8, jitter=record_jitter, sleep=clock.sleep, monotonic=clock.monotonic
    )
    with pytest.raises(ConnectionError):
        retry(lambda: (_ for _ in ()).throw(ConnectionError("down")), policy=policy)

    assert [low for low, _ in sampled] == [0.0] * 8
    expected = [
        min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (n - 1))) for n in range(1, 9)
    ]
    assert [high for _, high in sampled] == expected
    assert expected[-3:] == [30.0, 30.0, 30.0]


@pytest.mark.parametrize(
    "error",
    [ConnectionError("x"), TimeoutError("x"), BrokenPipeError("x"), _Vendor("429")],
)
def test_recognized_transient_failures_are_retried(error: BaseException) -> None:
    assert default_classifier(
        FailureContext(exception=error, attempt=1, scope="s", elapsed_seconds=0.0)
    )


@pytest.mark.parametrize(
    "error",
    [
        ValueError("invalid input"),
        TypeError("programming error"),
        PermissionError("denied"),
        KeyError("missing"),
        NotImplementedError("nope"),
    ],
)
def test_permanent_failures_are_not_retried(error: BaseException) -> None:
    assert not default_classifier(
        FailureContext(exception=error, attempt=1, scope="s", elapsed_seconds=0.0)
    )


def test_cancellation_and_termination_are_never_transient() -> None:
    clock = _Clock()
    attempts = {"n": 0}

    def cancelled() -> None:
        attempts["n"] += 1
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        retry(cancelled, policy=_policy(clock))
    assert attempts["n"] == 1


def test_a_more_specific_permanent_class_beats_a_broad_transport_category() -> None:
    # PermissionError is an OSError, as ConnectionError is; permanent wins.
    assert not default_classifier(
        FailureContext(
            exception=PermissionError("denied"), attempt=1, scope="s", elapsed_seconds=0.0
        )
    )


def test_arbitrary_error_text_is_not_classified_as_transient() -> None:
    assert not default_classifier(
        FailureContext(
            exception=Exception("connection timeout rate limit"),
            attempt=1,
            scope="s",
            elapsed_seconds=0.0,
        )
    )


# ---------------------------------------------------------------------------
# One policy at every boundary (coverage 3)
# ---------------------------------------------------------------------------


def test_the_same_policy_works_at_operation_record_and_batch_boundaries() -> None:
    clock = _Clock()
    policy = _policy(clock)
    calls = {"operation": 0, "record": 0, "batch": 0}

    def flaky(name: str) -> Any:
        def inner() -> str:
            calls[name] += 1
            if calls[name] < 3:
                raise ConnectionError("down")
            return name

        return inner

    assert retry(flaky("operation"), policy=policy, scope="operation") == "operation"

    def enrich_batch(records: list[int]) -> list[str]:
        return [retry(flaky("record"), policy=policy, scope=f"record-{value}") for value in records]

    assert retry(lambda: enrich_batch([1]), policy=policy, scope="batch") == ["record"]
    assert calls["operation"] == 3
    assert calls["record"] == 3


# ---------------------------------------------------------------------------
# Custom predicates and configuration (coverage 4)
# ---------------------------------------------------------------------------


def test_a_custom_predicate_extends_the_default_classifier() -> None:
    clock = _Clock()
    default = RetryPolicy()

    def retryable(failure: FailureContext) -> bool:
        error = failure.exception
        return default.should_retry(failure) or (
            isinstance(error, _Vendor) and error.code == "TEMPORARILY_BUSY"
        )

    attempts = {"n": 0}

    def busy() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _Vendor("TEMPORARILY_BUSY")
        return "ok"

    assert retry(busy, policy=_policy(clock, when=retryable)) == "ok"
    assert attempts["n"] == 3


def test_a_replacement_classifier_overrides_the_default() -> None:
    clock = _Clock()
    attempts = {"n": 0}

    def never_retry(failure: FailureContext) -> bool:
        del failure
        return False

    def transient() -> None:
        attempts["n"] += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        retry(transient, policy=_policy(clock, when=never_retry))
    assert attempts["n"] == 1


def test_zero_retries_disables_retrying() -> None:
    clock = _Clock()
    attempts = {"n": 0}

    def transient() -> None:
        attempts["n"] += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        retry(transient, policy=_policy(clock, max_retries=0))
    assert attempts["n"] == 1
    assert clock.slept == []


def test_a_failing_predicate_becomes_a_policy_error_not_a_retry() -> None:
    clock = _Clock()

    def broken(failure: FailureContext) -> bool:
        raise RuntimeError("predicate exploded")

    with pytest.raises(RetryPolicyError, match="predicate failures are policy errors"):
        retry(
            lambda: (_ for _ in ()).throw(ConnectionError("down")),
            policy=_policy(clock, when=broken),
        )


def test_a_successful_return_is_accepted_unless_a_result_classifier_opts_in() -> None:
    clock = _Clock()
    assert retry(lambda: {"ok": False}, policy=_policy(clock)) == {"ok": False}

    attempts = {"n": 0}

    def returns_failure() -> dict[str, Any]:
        attempts["n"] += 1
        return {"ok": attempts["n"] >= 3}

    result = retry(
        returns_failure,
        policy=_policy(clock, on_result=lambda value: not value["ok"]),
    )
    assert result == {"ok": True}
    assert attempts["n"] == 3


def test_a_result_classifier_exhausting_attempts_raises_retry_error() -> None:
    clock = _Clock()
    with pytest.raises(RetryError, match="returned a failure result"):
        retry(lambda: {"ok": False}, policy=_policy(clock, on_result=lambda value: True))


def test_a_failing_result_classifier_is_a_policy_error() -> None:
    clock = _Clock()

    def broken(value: Any) -> bool:
        raise RuntimeError("classifier exploded")

    with pytest.raises(RetryPolicyError, match="policy errors"):
        retry(lambda: "value", policy=_policy(clock, on_result=broken))


def test_policy_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        RetryPolicy(max_retries=-1)
    with pytest.raises(TypeError, match="max_retries"):
        RetryPolicy(max_retries="three")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="when"):
        RetryPolicy(when="nope")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="on_result"):
        RetryPolicy(on_result="nope")  # type: ignore[arg-type]


def test_max_elapsed_time_stops_further_attempts() -> None:
    clock = _Clock()
    attempts = {"n": 0}

    def transient() -> None:
        attempts["n"] += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        retry(transient, policy=_policy(clock, max_retries=10, max_elapsed_seconds=3.0))
    assert attempts["n"] < 11


# ---------------------------------------------------------------------------
# Nesting, budgets, and deadlines (coverage 5 and 6)
# ---------------------------------------------------------------------------


def test_a_nested_call_acquires_no_policy_automatically() -> None:
    clock = _Clock()
    inner_calls = {"n": 0}

    def inner() -> None:
        inner_calls["n"] += 1
        raise ConnectionError("down")

    def outer() -> None:
        inner()

    with pytest.raises(ConnectionError):
        retry(outer, policy=_policy(clock))

    # Only the outer boundary retried; the inner call got no policy of its own.
    assert inner_calls["n"] == 4


def test_nested_policies_share_one_retry_budget() -> None:
    clock = _Clock()
    budget = RetryBudget(remaining=2)
    policy = _policy(clock)

    def always_fails() -> None:
        raise ConnectionError("down")

    with pytest.raises(RetryBudgetExhaustedError):
        retry(always_fails, policy=policy, scope="inner", budget=budget)
    assert budget.remaining == 0


def test_an_exhausted_budget_prevents_an_enclosing_policy_from_restarting() -> None:
    clock = _Clock()
    budget = RetryBudget(remaining=1)
    policy = _policy(clock)
    outer_attempts = {"n": 0}

    def inner() -> None:
        raise ConnectionError("down")

    def outer() -> None:
        outer_attempts["n"] += 1
        retry(inner, policy=policy, scope="inner", budget=budget)

    with pytest.raises(RetryBudgetExhaustedError):
        retry(outer, policy=policy, scope="outer", budget=budget)
    assert outer_attempts["n"] == 1


def test_a_retry_stops_rather_than_overrunning_the_remaining_budget() -> None:
    clock = _Clock()
    budget = RetryBudget(deadline_seconds=0.5, shutdown_margin_seconds=0.2)

    def always_fails() -> None:
        raise ConnectionError("down")

    with pytest.raises(RetryCancelled, match="durable boundary"):
        retry(always_fails, policy=_policy(clock), budget=budget)
    assert clock.slept == [], "no attempt started that could not fit"


def test_a_budget_with_room_permits_the_attempt() -> None:
    clock = _Clock()
    budget = RetryBudget(deadline_seconds=600.0, shutdown_margin_seconds=1.0)
    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise ConnectionError("down")
        return "ok"

    assert retry(flaky, policy=_policy(clock), budget=budget) == "ok"
    assert attempts["n"] == 2


def test_retry_budget_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="remaining"):
        RetryBudget(remaining=-1)
    with pytest.raises(TypeError, match="remaining"):
        RetryBudget(remaining="many")  # type: ignore[arg-type]


def test_failure_context_exposes_scope_attempt_and_error_code() -> None:
    seen: list[FailureContext] = []

    def capture(failure: FailureContext) -> bool:
        seen.append(failure)
        return failure.attempt < 2

    clock = _Clock()
    with pytest.raises(_Vendor):
        retry(
            lambda: (_ for _ in ()).throw(_Vendor("429")),
            policy=_policy(clock, when=capture),
            scope="node/p1/batch-0",
        )
    assert [failure.attempt for failure in seen] == [1, 2]
    assert seen[0].scope == "node/p1/batch-0"
    assert seen[0].error_code == "429"
