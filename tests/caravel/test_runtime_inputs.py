"""Execution-level tests for the runtime input contract.

The declaration-level contract lives in ``test_step_inputs.py``. These cover
what the durable engine does with a processor once it runs: partition identity,
barrier readiness, batching, explicit retries inside processing, control
isolation, resume, and declaration-fingerprint rejection.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import (
    DurableStorage,
    JSONDataset,
    Pipeline,
    RetryOptions,
    Stage,
    Step,
    StepInputs,
    retry_operation,
    run,
)
from caravel.durability import AGGREGATE_PARTITION_ID, InMemorySource

PIPELINE = "managed-inputs"


@pytest.fixture()
def storage(tmp_path: Path) -> Iterator[Any]:
    with DurableStorage.at(tmp_path / "state") as opened:
        yield opened


def source() -> InMemorySource:
    return InMemorySource({"a": {"value": 1}, "b": {"value": 2}})


# -- builders used across the tests ------------------------------------------


def enrich(inputs: StepInputs, *, context: Any) -> Any:
    [partition] = inputs.partitions()
    del context
    payload = partition.load()
    return {**payload, "enriched": True}


def summarize(inputs: StepInputs, *, context: Any) -> Any:
    del context
    return {
        "count": len(inputs),
        "ids": sorted(p.partition_id for p in inputs.partitions()),
    }


def two_stage_pipeline() -> Pipeline:
    return Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich, output=JSONDataset())]),
            Stage(
                name="combine", entries=[Step(summarize, input_scope="all", output=JSONDataset())]
            ),
        ],
    )


# -- execution ---------------------------------------------------------------


def test_each_preserves_partition_identity_through_execution(storage: Any) -> None:
    seen: list[str] = []

    def observe(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        seen.append(partition.partition_id)
        return partition.load()

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(observe)])])
    result = run(pipeline, job_id="each-1", source=source(), storage=storage)

    assert result.status == "completed"
    assert sorted(seen) == ["a", "b"]


def test_each_handle_exposes_partition_and_batch_identity(storage: Any) -> None:
    handles: list[tuple[str, str]] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        handles.append((partition.partition_id, partition.batch_id))
        return partition.load()

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="each-2", source=source(), storage=storage)

    assert sorted(handles) == [("a", "batch-000000"), ("b", "batch-000000")]


def test_all_declares_the_aggregate_barrier(storage: Any) -> None:
    result = run(two_stage_pipeline(), job_id="all-1", source=source(), storage=storage)
    assert result.status == "completed"

    # The barrier's output is addressed by its declared name, never by the
    # private aggregate execution ID.
    results = storage.results(two_stage_pipeline(), job_id="all-1")
    assert list(results.of("summarize")) == ["summarize"]
    summary = results.of("summarize")["summarize"]
    assert summary["count"] == 2
    assert summary["ids"] == ["a", "b"]
    # The reserved identity stays where it belongs: recovery state.
    assert storage.progress.work_unit_partitions("all-1", "stage-002-entry-001") == (
        AGGREGATE_PARTITION_ID,
    )


def test_all_callback_runs_once_after_every_upstream_commit(storage: Any) -> None:
    invocations: list[int] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        del context
        invocations.append(len(inputs))
        return {"count": len(inputs)}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, input_scope="all", name="builder")]),
        ],
    )
    run(pipeline, job_id="all-2", source=source(), storage=storage)
    assert invocations == [2]


def test_processors_receive_runtime_context(storage: Any) -> None:
    observed: list[Any] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        observed.append(context)
        return partition.load()

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="ctx-1", source=source(), storage=storage, params={"mode": "x"})

    assert observed
    runtime = observed[0]
    assert runtime.step_name == "builder"
    assert runtime.run_id == "ctx-1"
    assert runtime.params == {"mode": "x"}
    assert hasattr(runtime, "run_root")


def test_ordinary_python_loops_are_supported_inside_callbacks(storage: Any) -> None:
    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        payload = partition.load()
        total = 0
        for _ in range(3):
            total += int(payload["value"])
        return {"total": total}

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder, name="m")])])
    run(pipeline, job_id="loop-1", source=source(), storage=storage)

    assert storage.results(pipeline, job_id="loop-1").of("m")["a"] == {"total": 3}


def test_retry_operation_inside_an_each_callback(storage: Any) -> None:
    attempts: list[int] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context

        def call() -> Any:
            attempts.append(1)
            if len(attempts) < 2:
                raise ConnectionError("transient")
            return partition.load()

        result, count = retry_operation(
            operation=call, retry=RetryOptions(max_attempts=3, delay_seconds=lambda _: 0.0)
        )
        assert count == 2
        return result

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(builder, name="builder")])],
    )
    result = run(
        pipeline,
        job_id="retry-1",
        source=InMemorySource({"a": {"value": 1}}),
        storage=storage,
    )
    assert result.status == "completed"
    assert len(attempts) == 2


def test_retry_operation_inside_an_all_callback(storage: Any) -> None:
    attempts: list[int] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        del context
        total = 0
        for partition in inputs.partitions():

            def call(view: Any = partition) -> Any:
                attempts.append(1)
                if len(attempts) < 2:
                    raise TimeoutError("transient")
                return view.load()

            payload, _ = retry_operation(
                operation=call, retry=RetryOptions(max_attempts=3, delay_seconds=lambda _: 0.0)
            )
            total += int(payload["value"])
        return {"total": total}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, input_scope="all", name="agg")]),
        ],
    )
    run(pipeline, job_id="retry-2", source=source(), storage=storage)

    assert storage.results(pipeline, job_id="retry-2").of("agg")["agg"] == {"total": 3}


def test_an_explicit_partition_retry_repeats_only_the_failed_partition(storage: Any) -> None:
    """The removed helper boundary is an explicit retry_operation call now."""
    calls: dict[str, int] = {}

    def flaky(partition_id: str, payload: Any) -> Any:
        calls[partition_id] = calls.get(partition_id, 0) + 1
        if partition_id == "b" and calls["b"] == 1:
            raise ConnectionError("transient")
        return payload

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        del context
        [partition] = inputs.partitions()
        value, _ = retry_operation(
            operation=partial(flaky, partition.partition_id, partition.load()),
            retry=RetryOptions(max_attempts=3, delay_seconds=lambda _: 0.0),
        )
        return value

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    result = run(pipeline, job_id="helper-retry", source=source(), storage=storage)

    assert result.status == "completed"
    assert calls == {"a": 1, "b": 2}


def test_managed_retry_inherits_controls_without_a_context_argument(storage: Any) -> None:
    """A nested operation retry needs no user-supplied execution context."""
    observed: list[bool] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        from caravel.retry import _active_retry_control

        observed.append(_active_retry_control.get() is not None)
        return partition.load()

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="controls-1", source=source(), storage=storage)
    assert observed and all(observed)


def test_execution_control_is_reset_after_the_invocation(storage: Any) -> None:
    from caravel.retry import _active_retry_control

    run(two_stage_pipeline(), job_id="controls-2", source=source(), storage=storage)
    assert _active_retry_control.get() is None


def test_execution_control_is_reset_after_a_failure(storage: Any) -> None:
    from caravel.retry import _active_retry_control

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context, partition
        raise RuntimeError("callback failed")

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(RuntimeError):
        run(pipeline, job_id="controls-3", source=source(), storage=storage)
    assert _active_retry_control.get() is None


def test_committed_callbacks_are_not_repeated_on_resume(storage: Any, tmp_path: Path) -> None:
    calls: list[str] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        calls.append(partition.partition_id)
        return partition.load()

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="resume-1", source=source(), storage=storage)
    first = list(calls)
    run(pipeline, job_id="resume-1", source=source(), storage=storage)

    assert calls == first


def test_each_batching_preserves_durable_batch_identities(storage: Any) -> None:
    batches: list[str] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        batches.append(partition.batch_id)
        return partition.load()

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(builder, batch_size=1, output=JSONDataset())])],
    )
    run(
        pipeline,
        job_id="batch-1",
        source=InMemorySource({"a": [{"value": 1}, {"value": 2}]}),
        storage=storage,
    )
    assert batches == ["batch-000000", "batch-000001"]


def test_all_batching_slices_partition_views(storage: Any) -> None:
    """An `all` batch reader exposes a contiguous slice of partition views."""
    sizes: list[int] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        del context
        sizes.append(len(inputs))
        return {"ids": sorted(p.partition_id for p in inputs.partitions())}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(
                name="combine", entries=[Step(builder, input_scope="all", name="agg", batch_size=1)]
            ),
        ],
    )
    run(pipeline, job_id="batch-2", source=source(), storage=storage)
    assert sizes == [1, 1]


def test_empty_sealed_input_gives_an_empty_aggregate_reader(storage: Any) -> None:
    observed: list[bool] = []

    def builder(inputs: StepInputs, *, context: Any) -> Any:
        del context
        observed.append(inputs.is_empty)
        return {"count": len(inputs)}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, input_scope="all", name="agg")]),
        ],
    )
    result = run(pipeline, job_id="empty-1", source=InMemorySource({}), storage=storage)
    assert result.status == "completed"
    assert observed == [True]


def test_upstream_failure_keeps_the_barrier_closed(storage: Any) -> None:
    reached: list[str] = []

    def upstream(inputs: StepInputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context, partition
        raise RuntimeError("upstream failed")

    def downstream(inputs: StepInputs, *, context: Any) -> Any:
        del context, inputs
        reached.append("downstream")
        return {}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(upstream)]),
            Stage(name="combine", entries=[Step(downstream, input_scope="all", name="agg")]),
        ],
    )
    with pytest.raises(RuntimeError):
        run(pipeline, job_id="closed-1", source=source(), storage=storage)
    assert reached == []


@pytest.mark.parametrize("change", ["scope", "revision", "dataset_name", "stage_name"])
def test_late_declaration_changes_cannot_hide_beyond_fingerprint_limit(
    storage: Any, change: str
) -> None:
    def process(inputs: StepInputs, *, context: Any) -> Any:
        return {"value": sum(partition.load()["value"] for partition in inputs.partitions())}

    def build() -> Pipeline:
        return Pipeline(
            name="long-plan",
            stages=[
                Stage(
                    name=f"stage-{index}-" + "x" * 64,
                    entries=[Step(process, name=f"process-{index}")],
                )
                for index in range(8)
            ],
        )

    original = build()
    run(original, job_id="long", source=source(), storage=storage)
    before = storage.progress.load_job("long")
    changed = build()
    last_stage = changed.stages[-1]
    last = last_stage.entries[-1]
    if change == "scope":
        last.input_scope = "all"
    elif change == "revision":
        last.revision = "2"
    elif change == "dataset_name":
        last.dataset.name = "renamed"
    else:
        last_stage.name = "renamed-stage"
    with pytest.raises(caravel.DurabilityError, match="fingerprint"):
        run(changed, job_id="long", source=source(), storage=storage)
    assert storage.progress.load_job("long") == before


def test_incompatible_persisted_declaration_fails_before_mutation(storage: Any) -> None:
    """Changing a step's declared scope fails closed and asks for a new job id."""
    run(two_stage_pipeline(), job_id="fingerprint-1", source=source(), storage=storage)

    changed = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich, output=JSONDataset())]),
            Stage(name="combine", entries=[Step(enrich, name="summarize")]),
        ],
    )
    with pytest.raises(caravel.DurabilityError):
        run(changed, job_id="fingerprint-1", source=source(), storage=storage)


# -- removed surfaces --------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["RetryPolicy", "RetryBudget", "FailureContext", "default_classifier", "retry"],
)
def test_superseded_public_retry_imports_are_gone(name: str) -> None:
    """No alias, shim, or compatibility re-export survives the replacement."""
    assert name not in caravel.__all__
    with pytest.raises(ImportError):
        exec(f"from caravel.retry import {name}")


def test_the_old_retry_wrapper_is_not_a_callable_export() -> None:
    """`caravel.retry` is the module; the retired retry() function is gone."""
    import types

    assert isinstance(caravel.retry, types.ModuleType)
    assert not callable(getattr(caravel.retry, "retry", None))


def test_the_replacement_retry_surface_is_public() -> None:
    for name in (
        "RetryOptions",
        "retry_operation",
        "compute_retry_delay",
        "RetryError",
        "RetryPolicyError",
        "RetryResultExhaustedError",
        "RetryCancelledError",
        "RetryBudgetExhaustedError",
    ):
        assert hasattr(caravel, name), name
        assert name in caravel.__all__, name


def test_the_runtime_input_surface_is_public() -> None:
    for name in ("StepInputs", "InputPartition", "StepInputsError", "InputScope"):
        assert hasattr(caravel, name), name
        assert name in caravel.__all__, name


def test_the_committed_dataset_reader_is_gone() -> None:
    """Neither the ST-10 reader nor the pre-ST-10 one is aliased back."""
    import caravel.durability as durability

    assert not hasattr(durability, "CommittedDatasetReader")
    assert not hasattr(durability, "AggregateInput")
