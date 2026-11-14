"""One retry contract: RetryOptions and retry_operation at every boundary."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    DurabilityOptions,
    Pipeline,
    RetryError,
    RetryOptions,
    RetryPolicyError,
    RetryResultExhaustedError,
    Stage,
    Step,
    compute_retry_delay,
    retry_operation,
    run,
)
from caravel.durability import InMemorySource
from caravel.retry import DEFAULT_MAX_ATTEMPTS, MAX_RETRY_DELAY_SECONDS

NO_DELAY = RetryOptions(delay_seconds=lambda attempt: 0.0)


class _Vendor(Exception):
    """A domain failure that is not a recognized transient class."""


def _counting(failures: int, error: BaseException) -> Any:
    """Return an operation that raises ``error`` the first ``failures`` times."""
    calls = {"n": 0}

    def operation() -> int:
        calls["n"] += 1
        if calls["n"] <= failures:
            raise error
        return calls["n"]

    operation.calls = calls  # type: ignore[attr-defined]
    return operation


# -- defaults and delay ------------------------------------------------------


def test_the_default_options_allow_four_total_attempts() -> None:
    assert RetryOptions().max_attempts == DEFAULT_MAX_ATTEMPTS == 4
    operation = _counting(3, ConnectionError("down"))
    result, attempts = retry_operation(operation=operation, retry=NO_DELAY)
    assert (result, attempts) == (4, 4)


def test_omitting_options_uses_the_documented_defaults() -> None:
    """retry_operation() with no options still retries under RetryOptions()."""
    operation = _counting(0, ConnectionError("down"))
    assert retry_operation(operation=operation) == (1, 1)


def test_delay_is_capped_exponential_plus_jitter() -> None:
    for attempt in range(1, 12):
        delay = compute_retry_delay(attempt)
        assert 0.0 <= delay <= MAX_RETRY_DELAY_SECONDS + 1.0
    assert 1.0 <= compute_retry_delay(1) < 2.0
    assert 2.0 <= compute_retry_delay(2) < 3.0
    # The exponential component caps at 16, so total delay never exceeds 17.
    assert 16.0 <= compute_retry_delay(9) < 17.0


def test_delay_rejects_a_non_positive_attempt_number() -> None:
    for bad in (0, -1):
        with pytest.raises(ValueError):
            compute_retry_delay(bad)


# -- classification ----------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [ConnectionError("reset"), TimeoutError("slow"), BrokenPipeError("broken")],
)
def test_recognized_transient_failures_are_retried(error: BaseException) -> None:
    assert RetryOptions().should_retry_error(error) is True


@pytest.mark.parametrize(
    "error",
    [ValueError("bad"), TypeError("bad"), PermissionError("denied"), KeyError("missing")],
)
def test_permanent_failures_are_not_retried(error: BaseException) -> None:
    assert RetryOptions().should_retry_error(error) is False


def test_control_signals_propagate_without_retry() -> None:
    """BaseException control signals are never caught, let alone classified."""

    def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        retry_operation(operation=interrupted, retry=NO_DELAY)

    def exiting() -> None:
        raise SystemExit(1)

    with pytest.raises(SystemExit):
        retry_operation(operation=exiting, retry=NO_DELAY)


def test_a_more_specific_permanent_class_beats_a_broad_transport_category() -> None:
    # PermissionError is an OSError, as ConnectionError is, but it is permanent.
    assert RetryOptions().should_retry_error(PermissionError("denied")) is False


def test_arbitrary_error_text_is_not_classified_as_transient() -> None:
    assert RetryOptions().should_retry_error(_Vendor("temporarily unavailable")) is False


def test_a_rate_limit_status_code_is_transient() -> None:
    error = _Vendor("slow down")
    error.status_code = 429  # type: ignore[attr-defined]
    assert RetryOptions().should_retry_error(error) is True


# -- predicates and validation -----------------------------------------------


def test_a_custom_exception_predicate_receives_the_bare_exception() -> None:
    seen: list[BaseException] = []

    def predicate(error: BaseException) -> bool:
        seen.append(error)
        return isinstance(error, _Vendor)

    operation = _counting(1, _Vendor("retry me"))
    result, attempts = retry_operation(
        operation=operation,
        retry=RetryOptions(should_retry_error=predicate, delay_seconds=lambda a: 0.0),
    )
    assert (result, attempts) == (2, 2)
    assert len(seen) == 1 and isinstance(seen[0], _Vendor)


def test_max_attempts_of_one_permits_the_initial_call_without_retry() -> None:
    operation = _counting(1, ConnectionError("down"))
    with pytest.raises(ConnectionError):
        retry_operation(operation=operation, retry=RetryOptions(max_attempts=1))
    assert operation.calls["n"] == 1  # type: ignore[attr-defined]


def test_a_failing_exception_predicate_is_a_policy_error() -> None:
    def predicate(error: BaseException) -> bool:
        raise RuntimeError("predicate exploded")

    with pytest.raises(RetryPolicyError, match="Error predicate failed"):
        retry_operation(
            operation=_counting(1, ConnectionError("down")),
            retry=RetryOptions(should_retry_error=predicate),
        )


def test_a_successful_return_is_accepted_unless_a_result_predicate_opts_in() -> None:
    assert retry_operation(operation=lambda: "ok", retry=NO_DELAY) == ("ok", 1)


def test_a_result_predicate_receives_the_bare_result() -> None:
    results = {"n": 0}

    def operation() -> str:
        results["n"] += 1
        return "pending" if results["n"] < 3 else "done"

    value, attempts = retry_operation(
        operation=operation,
        retry=RetryOptions(
            should_retry_result=lambda result: result == "pending",
            delay_seconds=lambda a: 0.0,
        ),
    )
    assert (value, attempts) == ("done", 3)


def test_an_exhausted_result_retry_raises_and_never_returns_the_value() -> None:
    with pytest.raises(RetryResultExhaustedError) as caught:
        retry_operation(
            operation=lambda: {"secret": "rejected-payload"},
            retry=RetryOptions(
                should_retry_result=lambda result: True,
                max_attempts=2,
                delay_seconds=lambda a: 0.0,
            ),
        )
    # No raw rejected result or payload appears in the exhaustion error.
    assert "secret" not in str(caught.value)
    assert "rejected-payload" not in str(caught.value)
    assert isinstance(caught.value, RetryError)


def test_a_failing_result_predicate_is_a_policy_error() -> None:
    def predicate(result: Any) -> bool:
        raise RuntimeError("predicate exploded")

    with pytest.raises(RetryPolicyError, match="Result predicate failed"):
        retry_operation(operation=lambda: "ok", retry=RetryOptions(should_retry_result=predicate))


def test_options_are_validated_at_construction() -> None:
    with pytest.raises(ValueError):
        RetryOptions(max_attempts=0)
    with pytest.raises(TypeError):
        RetryOptions(max_attempts="four")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RetryOptions(max_attempts=True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RetryOptions(should_retry_error="always")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RetryOptions(should_retry_result="always")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RetryOptions(delay_seconds=1.0)  # type: ignore[arg-type]


def test_an_invalid_delay_is_a_policy_error() -> None:
    for bad in (-1.0, float("inf"), float("nan"), True, "soon"):
        with pytest.raises(RetryPolicyError, match="finite and nonnegative"):
            retry_operation(
                operation=_counting(1, ConnectionError("down")),
                retry=RetryOptions(delay_seconds=lambda attempt, value=bad: value),  # type: ignore
            )


def test_a_failing_delay_function_is_a_policy_error() -> None:
    def delay(attempt: int) -> float:
        raise RuntimeError("clock exploded")

    with pytest.raises(RetryPolicyError, match="delay calculation failed"):
        retry_operation(
            operation=_counting(1, ConnectionError("down")),
            retry=RetryOptions(delay_seconds=delay),
        )


@pytest.mark.parametrize("boundary", ["stage", "step", "operation"])
def test_dictionary_based_retry_configuration_is_rejected(boundary: str) -> None:
    invalid_options: Any = {"max_attempts": 2}

    def processor(inputs: Any, *, context: Any) -> Any:
        del context
        [value] = inputs.partitions()
        return value.load()

    with pytest.raises(TypeError, match=r"retry.*RetryOptions"):
        if boundary == "stage":
            Stage(name="s", entries=[Step(processor)], retry=invalid_options)
        elif boundary == "step":
            Step(processor, retry=invalid_options)
        else:
            retry_operation(operation=lambda: 1, retry=invalid_options)


def test_partial_operation_retries_with_bound_arguments() -> None:
    calls: list[str] = []

    def operation(record: str, *, model: str) -> str:
        calls.append(record)
        if len(calls) == 1:
            raise ConnectionError("temporary failure")
        return f"{model}:{record}"

    result, attempts = retry_operation(
        operation=partial(operation, "record-1", model="test-model"), retry=NO_DELAY
    )
    assert (result, attempts) == ("test-model:record-1", 2)
    assert calls == ["record-1", "record-1"]


def test_retry_operation_rejects_a_non_callable_before_attempting_it() -> None:
    invalid_operation: Any = 42
    with pytest.raises(TypeError, match="operation must be callable"):
        retry_operation(operation=invalid_operation)


def test_retry_args_keyword_is_rejected() -> None:
    with pytest.raises(TypeError):
        retry_operation(operation=lambda: 1, retry_args={"max_attempts": 2})  # type: ignore[call-arg]


# -- one instance, every boundary --------------------------------------------

# --- retries through the engine ------------------------------------------


def _pipeline(step, *, job_id="retry-1", stage_retry=None, source=None):
    return Pipeline(
        name="retry_example",
        stages=[Stage(name="only", entries=[step], retry=stage_retry)],
        source=source or InMemorySource({"a": 1}),
        durability_options=DurabilityOptions(
            schema_version="v1.0.0", job_id=job_id, commit_scope="step"
        ),
    )


NO_DELAY = RetryOptions(max_attempts=4, delay_seconds=lambda attempt: 0.0)


def test_a_step_retry_repeats_the_processor(tmp_path: Path) -> None:
    attempts: list[int] = []

    def flaky(value: Any, *, context: Any) -> Any:
        attempts.append(1)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return {"attempts": len(attempts)}

    run(_pipeline(Step(flaky, retry=NO_DELAY)), output=tmp_path / "output")
    assert len(attempts) == 2


def test_a_stage_retry_repeats_every_unfinished_step(tmp_path: Path) -> None:
    calls: list[str] = []
    failed: list[int] = []

    def first(value: Any, *, context: Any) -> Any:
        calls.append("first")
        return value

    def second(value: Any, *, context: Any) -> Any:
        calls.append("second")
        if not failed:
            failed.append(1)
            raise ConnectionError("transient")
        return value

    pipeline = Pipeline(
        name="retry_stage",
        stages=[
            Stage(
                name="only",
                entries=[Step(first, name="first"), Step(second, name="second")],
                retry=NO_DELAY,
            )
        ],
        source=InMemorySource({"a": 1}),
        durability_options=DurabilityOptions(
            schema_version="v1.0.0", job_id="retry-stage", commit_scope="pipeline"
        ),
    )
    run(pipeline, output=tmp_path / "output")
    assert calls == ["first", "second", "first", "second"]


def test_the_same_options_drive_retry_operation_inside_a_processor(tmp_path: Path) -> None:
    attempts: list[int] = []

    def inner() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("transient")
        return "done"

    def processor(value: Any, *, context: Any) -> Any:
        result, count = retry_operation(operation=inner, retry=NO_DELAY)
        return {"result": result, "attempts": count}

    run(_pipeline(Step(processor)), output=tmp_path / "output")
    assert len(attempts) == 3


def test_an_exhausted_step_retry_surfaces_the_last_failure(tmp_path: Path) -> None:
    attempts: list[int] = []

    def always_failing(value: Any, *, context: Any) -> Any:
        attempts.append(1)
        raise ConnectionError("still transient")

    options = RetryOptions(max_attempts=3, delay_seconds=lambda attempt: 0.0)
    with pytest.raises(ConnectionError, match="still transient"):
        run(_pipeline(Step(always_failing, retry=options)), output=tmp_path / "output")
    assert len(attempts) == 3


def test_a_retried_step_commits_only_its_accepted_output(tmp_path: Path) -> None:
    import sqlite3

    attempts: list[int] = []

    def flaky(value: Any, *, context: Any) -> Any:
        attempts.append(1)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return {"attempts": len(attempts)}

    run(_pipeline(Step(flaky, name="flaky", retry=NO_DELAY)), output=tmp_path / "output")

    connection = sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3")
    snapshot = connection.execute(
        "SELECT snapshot FROM steps WHERE step_name = 'flaky'"
    ).fetchone()[0]
    connection.close()
    assert b'"attempts": 2' in snapshot
