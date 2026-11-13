"""One retry contract: RetryOptions and retry_operation at every boundary."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    DurableStorage,
    Inputs,
    ManagedInputError,
    Pipeline,
    RetryBudgetExhaustedError,
    RetryCancelledError,
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
from caravel.durability import ExecutionBudget, ExecutionPolicy, InMemorySource
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
                retry=RetryOptions(delay_seconds=lambda attempt, value=bad: value),
            )


def test_a_failing_delay_function_is_a_policy_error() -> None:
    def delay(attempt: int) -> float:
        raise RuntimeError("clock exploded")

    with pytest.raises(RetryPolicyError, match="delay calculation failed"):
        retry_operation(
            operation=_counting(1, ConnectionError("down")),
            retry=RetryOptions(delay_seconds=delay),
        )


@pytest.mark.parametrize("boundary", ["stage", "step", "each", "all", "operation"])
def test_dictionary_based_retry_configuration_is_rejected(boundary: str) -> None:
    invalid_options: Any = {"max_attempts": 2}

    def callback(value: Any, *, context: Any) -> Any:
        return value.load()

    def builder(inputs: Any, *, context: Any) -> Any:
        return inputs.each(callback)

    expected_error = ManagedInputError if boundary in ("each", "all") else TypeError
    with pytest.raises(expected_error, match=r"retry.*RetryOptions"):
        if boundary == "stage":
            Stage(name="s", entries=[Step(builder)], retry=invalid_options)
        elif boundary == "step":
            Step(builder, retry=invalid_options)
        elif boundary == "each":
            Inputs().each(callback, retry=invalid_options)
        elif boundary == "all":
            Inputs().all(callback, retry=invalid_options)
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


def test_managed_retry_wait_responds_to_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    from caravel.durability.execution import _Deadline, _InvocationControls

    controls = _InvocationControls(
        deadline=_Deadline(ExecutionBudget(seconds=30)), allowance=3, shutdown_margin=1
    )
    bound = controls.bind(scope="test", owner_id="worker", fence_token=None)
    waits: list[float] = []

    def cancel_during_wait(seconds: float) -> bool:
        waits.append(seconds)
        controls.cancel()
        return True

    def reject_blocking_sleep(seconds: float) -> None:
        raise AssertionError("managed retry used an uninterruptible sleep")

    monkeypatch.setattr(controls.cancel_event, "wait", cancel_during_wait)
    monkeypatch.setattr("caravel.durability.execution.time.sleep", reject_blocking_sleep)
    with pytest.raises(RetryCancelledError, match="cancelled"):
        bound.wait(10)
    assert waits and waits[0] <= 0.1


def test_retry_args_keyword_is_rejected() -> None:
    with pytest.raises(TypeError):
        retry_operation(operation=lambda: 1, retry_args={"max_attempts": 2})  # type: ignore[call-arg]


# -- one instance, every boundary --------------------------------------------


def _storage(tmp_path: Path) -> Any:
    return DurableStorage.at(tmp_path / "output")


def test_one_options_instance_is_reused_at_stage_step_helper_and_operation(
    tmp_path: Path,
) -> None:
    """The same RetryOptions object configures all four supported boundaries."""
    shared = RetryOptions(max_attempts=3, delay_seconds=lambda attempt: 0.0)
    attempts = {"operation": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context

        def call() -> Any:
            attempts["operation"] += 1
            if attempts["operation"] < 2:
                raise ConnectionError("vendor down")
            return partition.load()

        value, _ = retry_operation(operation=call, retry=shared)
        return value

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(callback, retry=shared)

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", retry=shared, entries=[Step(builder, retry=shared)]),
        ],
    )
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"p1": {"id": "p1"}}),
        storage=_storage(tmp_path),
    )
    assert result.status == "completed"
    assert attempts["operation"] == 2


def test_disabled_retry_does_not_repeat_a_failing_callback(tmp_path: Path) -> None:
    calls = {"n": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context, partition
        calls["n"] += 1
        raise ConnectionError("vendor down")

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(callback, retry=None)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(builder)])])
    with pytest.raises(ConnectionError):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"p1": {"id": "p1"}}),
            storage=_storage(tmp_path),
        )
    assert calls["n"] == 1


def test_inner_retries_exhaust_before_an_outer_boundary_restarts(tmp_path: Path) -> None:
    """Nesting is stage -> step -> helper, so the innermost exhausts first."""
    calls: list[str] = []

    def callback(partition: Any, *, context: Any) -> Any:
        del context, partition
        calls.append("callback")
        raise ConnectionError("vendor down")

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(
            callback, retry=RetryOptions(max_attempts=2, delay_seconds=lambda a: 0.0)
        )

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                retry=RetryOptions(max_attempts=2, delay_seconds=lambda a: 0.0),
                entries=[
                    Step(
                        builder,
                        retry=RetryOptions(max_attempts=2, delay_seconds=lambda a: 0.0),
                    )
                ],
            )
        ],
    )
    with pytest.raises(ConnectionError):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"p1": {"id": "p1"}}),
            storage=_storage(tmp_path),
        )
    # 2 helper attempts x 2 step attempts x 2 stage attempts.
    assert len(calls) == 8


def test_an_operation_retry_repeats_only_the_wrapped_call(tmp_path: Path) -> None:
    callback_calls = {"n": 0}
    operation_calls = {"n": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context
        callback_calls["n"] += 1

        def call() -> Any:
            operation_calls["n"] += 1
            if operation_calls["n"] < 3:
                raise ConnectionError("vendor down")
            return partition.load()

        value, _ = retry_operation(operation=call, retry=NO_DELAY)
        return value

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(callback)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(builder)])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"p1": {"id": "p1"}}),
        storage=_storage(tmp_path),
    )
    assert result.status == "completed"
    assert callback_calls["n"] == 1
    assert operation_calls["n"] == 3


def test_retry_operation_works_outside_the_runner() -> None:
    """Standalone use has its own local limits and claims no durable recovery."""
    from caravel.retry import _active_retry_control

    assert _active_retry_control.get() is None
    operation = _counting(2, TimeoutError("slow"))
    assert retry_operation(operation=operation, retry=NO_DELAY) == (3, 3)


# -- shared invocation controls ----------------------------------------------


def test_nested_retries_share_the_invocation_allowance(tmp_path: Path) -> None:
    """Explicitly nested policies cannot multiply attempts without bound."""
    calls = {"n": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context, partition

        def call() -> Any:
            calls["n"] += 1
            raise ConnectionError("vendor down")

        return retry_operation(
            operation=call, retry=RetryOptions(max_attempts=50, delay_seconds=lambda a: 0.0)
        )

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(callback)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(builder)])])
    with pytest.raises(RetryBudgetExhaustedError):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"p1": {"id": "p1"}}),
            storage=_storage(tmp_path),
            policy=ExecutionPolicy(max_retry_attempts=5),
        )
    # Five retries were charged; the initial attempt was not.
    assert calls["n"] == 6


def test_an_exhausted_allowance_is_terminal_for_the_enclosing_retry(tmp_path: Path) -> None:
    """Budget exhaustion propagates through outer policies, never reclassified."""
    calls = {"n": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context, partition

        def call() -> Any:
            calls["n"] += 1
            raise ConnectionError("vendor down")

        return retry_operation(
            operation=call, retry=RetryOptions(max_attempts=50, delay_seconds=lambda a: 0.0)
        )

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(
            callback, retry=RetryOptions(max_attempts=10, delay_seconds=lambda a: 0.0)
        )

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                retry=RetryOptions(max_attempts=10, delay_seconds=lambda a: 0.0),
                entries=[Step(builder)],
            )
        ],
    )
    with pytest.raises(RetryBudgetExhaustedError):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"p1": {"id": "p1"}}),
            storage=_storage(tmp_path),
            policy=ExecutionPolicy(max_retry_attempts=3),
        )
    assert calls["n"] == 4


def test_initial_attempts_do_not_consume_the_allowance(tmp_path: Path) -> None:
    """A zero allowance still permits every initial call across many partitions."""
    seen: list[str] = []

    def callback(partition: Any, *, context: Any) -> Any:
        del context
        seen.append(partition.partition_id)
        return partition.load()

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(callback, retry=NO_DELAY)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(builder)])])
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"a": {"v": 1}, "b": {"v": 2}, "c": {"v": 3}}),
        storage=_storage(tmp_path),
        policy=ExecutionPolicy(max_retry_attempts=0),
    )
    assert result.status == "completed"
    assert sorted(seen) == ["a", "b", "c"]


def test_a_retry_stops_rather_than_overrunning_the_invocation_deadline(
    tmp_path: Path,
) -> None:
    calls = {"n": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context, partition

        def call() -> Any:
            calls["n"] += 1
            raise ConnectionError("vendor down")

        # A delay far larger than the remaining budget must not be waited out.
        return retry_operation(
            operation=call, retry=RetryOptions(max_attempts=5, delay_seconds=lambda a: 600.0)
        )

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(callback)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(builder)])])
    with pytest.raises(RetryCancelledError):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"p1": {"id": "p1"}}),
            storage=_storage(tmp_path),
            budget=ExecutionBudget(seconds=30.0),
        )
    assert calls["n"] == 1


def test_execution_policy_validates_the_allowance() -> None:
    with pytest.raises(ValueError):
        ExecutionPolicy(max_retry_attempts=-1)
    with pytest.raises(TypeError):
        ExecutionPolicy(max_retry_attempts=True)  # type: ignore[arg-type]
    assert ExecutionPolicy().max_retry_attempts == 100


def test_controls_are_isolated_between_invocations(tmp_path: Path) -> None:
    """A failed invocation leaves the next one a fresh allowance and context."""
    from caravel.retry import _active_retry_control

    state = {"fail": True, "calls": 0}

    def callback(partition: Any, *, context: Any) -> Any:
        del context
        state["calls"] = int(state["calls"]) + 1
        if state["fail"]:
            raise ConnectionError("vendor down")
        return partition.load()

    def builder(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.each(
            callback, retry=RetryOptions(max_attempts=2, delay_seconds=lambda a: 0.0)
        )

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(builder)])])
    storage = _storage(tmp_path)
    source = InMemorySource({"p1": {"id": "p1"}})

    with pytest.raises(ConnectionError):
        run(pipeline, job_id="j1", source=source, storage=storage)
    assert _active_retry_control.get() is None

    state["fail"] = False
    result = run(pipeline, job_id="j1", source=source, storage=storage)
    assert result.status == "completed"
    assert _active_retry_control.get() is None
