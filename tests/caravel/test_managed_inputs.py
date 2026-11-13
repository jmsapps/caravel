"""Acceptance tests for the managed-input declaration contract.

These express the ST-10 contract directly: a processor is a deterministic
declaration builder that returns exactly one managed descriptor, builders are
compiled before source discovery or durable mutation, and callbacks run only
through the durable engine.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import (
    Branch,
    DurableStorage,
    JSONDataset,
    ManagedInputError,
    Pipeline,
    RetryOptions,
    Stage,
    Step,
    retry_operation,
    run,
)
from caravel.durability import (
    AGGREGATE_PARTITION_ID,
    InMemorySource,
    UnsupportedDurableDeclarationError,
)
from caravel.durability.scheduler import compile_durable_plan, declaration_fingerprint
from caravel.inputs import (
    AggregateInput,
    DeclarationContext,
    Inputs,
    ManagedDescriptor,
    PartitionInput,
)

PIPELINE = "managed-inputs"


@pytest.fixture()
def storage(tmp_path: Path) -> Iterator[Any]:
    with DurableStorage.at(tmp_path / "state") as opened:
        yield opened


def source() -> InMemorySource:
    return InMemorySource({"a": {"value": 1}, "b": {"value": 2}})


# -- builders used across the tests ------------------------------------------


def enrich_partition(partition: PartitionInput, *, context: Any) -> Any:
    del context
    payload = partition.load()
    return {**payload, "enriched": True}


def enrich(inputs: Inputs, *, context: Any) -> ManagedDescriptor:
    del context
    return inputs.each(enrich_partition)


def summarize_partitions(partitions: AggregateInput, *, context: Any) -> Any:
    del context
    return {
        "count": len(partitions),
        "ids": sorted(p.partition_id for p in partitions.partitions()),
    }


def summarize(inputs, *, context: Any) -> ManagedDescriptor:
    del context
    return inputs.all(summarize_partitions)


def two_stage_pipeline() -> Pipeline:
    return Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich, output=JSONDataset())]),
            Stage(name="combine", entries=[Step(summarize, output=JSONDataset())]),
        ],
    )


# -- declaration-time behavior -----------------------------------------------


def test_builder_returns_descriptor_without_invoking_callback() -> None:
    """Registration is side-effect free: `each` records, it does not iterate."""
    calls: list[Any] = []

    def record(partition: PartitionInput, *, context: Any) -> Any:
        calls.append(partition)
        return partition.load()

    builder = Inputs()
    descriptor = builder.each(record)

    assert isinstance(descriptor, ManagedDescriptor)
    assert descriptor.kind == "each"
    assert descriptor.callback is record
    assert calls == []


def test_all_registers_the_aggregate_barrier() -> None:
    builder = Inputs()
    descriptor = builder.all(summarize_partitions)
    assert descriptor.kind == "all"


def test_compilation_does_not_invoke_callbacks_or_touch_the_source() -> None:
    """Compiling a plan runs builders only; no callback and no discovery."""
    invoked: list[str] = []

    def callback(partition: PartitionInput, *, context: Any) -> Any:
        invoked.append("callback")
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        invoked.append("builder")
        return inputs.each(callback)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    compile_durable_plan(pipeline, params={})

    assert invoked == ["builder"]


def test_builder_is_compiled_exactly_once_per_declared_step() -> None:
    calls: list[str] = []

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        calls.append(context_free_marker)
        return inputs.each(enrich_partition)

    context_free_marker = "compiled"
    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    compile_durable_plan(pipeline, params={})
    assert calls == ["compiled"]


def test_recompilation_is_deterministic() -> None:
    """Resume recompiles the builder and must reconstruct the same descriptor."""
    pipeline = two_stage_pipeline()
    first = compile_durable_plan(pipeline, params={})
    second = compile_durable_plan(pipeline, params={})

    assert declaration_fingerprint(first) == declaration_fingerprint(second)
    assert [node.descriptor.kind for node in first.nodes] == [
        node.descriptor.kind for node in second.nodes
    ]


def test_declaration_params_cannot_be_mutated_or_changed_by_the_caller() -> None:
    params = {"mode": "original"}
    context = DeclarationContext(
        pipeline_name="p",
        stage_name="s",
        stage_index=1,
        step_name="x",
        step_index=1,
        params=params,
    )
    params["mode"] = "changed"
    assert context.params["mode"] == "original"
    invalid_mapping: Any = context.params
    with pytest.raises(TypeError):
        invalid_mapping["mode"] = "mutated"


def test_declaration_context_exposes_names_and_params_only() -> None:
    seen: list[Any] = []

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        seen.append(context)
        return inputs.each(enrich_partition)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="prepare", entries=[Step(builder)])])
    compile_durable_plan(pipeline, params={"mode": "backfill"})

    context = seen[0]
    assert context.pipeline_name == PIPELINE
    assert context.stage_name == "prepare"
    assert context.stage_index == 1
    assert context.step_name == "builder"
    assert context.step_index == 1
    assert dict(context.params) == {"mode": "backfill"}
    for forbidden in ("load", "partitions", "store", "worker_id", "retry", "fence_token"):
        assert not hasattr(context, forbidden)


def test_helper_kind_participates_in_the_fingerprint() -> None:
    """Switching each -> all is a changed declaration identity, not a resume."""

    def as_each(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(enrich_partition)

    def as_all(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(summarize_partitions)

    each_plan = compile_durable_plan(
        Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(as_each, name="n")])]),
        params={},
    )
    all_plan = compile_durable_plan(
        Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(as_all, name="n")])]),
        params={},
    )
    assert declaration_fingerprint(each_plan) != declaration_fingerprint(all_plan)


def test_revision_participates_in_the_fingerprint() -> None:
    first = compile_durable_plan(
        Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(enrich, revision="1")])]),
        params={},
    )
    second = compile_durable_plan(
        Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(enrich, revision="2")])]),
        params={},
    )
    assert declaration_fingerprint(first) != declaration_fingerprint(second)


def test_descriptor_identity_is_the_existing_step_node_id() -> None:
    plan = compile_durable_plan(two_stage_pipeline(), params={})
    assert [node.node_id for node in plan.nodes] == [
        "stage-001-entry-001",
        "stage-002-entry-001",
    ]


# -- rejected declarations ---------------------------------------------------


def test_second_helper_call_is_rejected() -> None:
    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        inputs.each(enrich_partition)
        return inputs.each(enrich_partition)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(ManagedInputError, match="exactly one"):
        compile_durable_plan(pipeline, params={})


def test_registration_loop_is_rejected() -> None:
    """A loop that registers helpers repeatedly is not supported composition."""

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        descriptor = None
        for _ in range(3):
            descriptor = inputs.each(enrich_partition)
        assert descriptor is not None
        return descriptor

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(ManagedInputError, match="exactly one"):
        compile_durable_plan(pipeline, params={})


def test_mixing_each_and_all_in_one_builder_is_rejected() -> None:
    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        inputs.each(enrich_partition)
        return inputs.all(summarize_partitions)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(ManagedInputError, match="exactly one"):
        compile_durable_plan(pipeline, params={})


def test_builder_returning_no_descriptor_is_rejected() -> None:
    def builder(inputs, *, context: Any) -> Any:
        del context, inputs
        return {"not": "a descriptor"}

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(ManagedInputError, match="must return"):
        compile_durable_plan(pipeline, params={})


def test_builder_returning_a_raw_payload_is_rejected() -> None:
    """A processor that computes instead of declaring fails at compilation."""

    def builder(inputs, *, context: Any) -> Any:
        del context, inputs
        return [{"value": 1}, {"value": 2}]

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(ManagedInputError, match="must return"):
        compile_durable_plan(pipeline, params={})


def test_foreign_descriptor_is_rejected() -> None:
    """A descriptor issued by another builder is not this step's declaration."""
    foreign = Inputs().each(enrich_partition)

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context, inputs
        return foreign

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(ManagedInputError, match="another declaration"):
        compile_durable_plan(pipeline, params={})


def test_builder_data_access_is_rejected() -> None:
    """The builder exposes no payload, storage handle, or iteration."""
    builder = Inputs()
    with pytest.raises(ManagedInputError):
        builder.load()  # type: ignore[attr-defined]
    with pytest.raises(ManagedInputError):
        list(builder)  # type: ignore[call-overload]
    with pytest.raises(ManagedInputError):
        len(builder)  # type: ignore[arg-type]
    with pytest.raises(ManagedInputError):
        builder["a"]  # type: ignore[index]


def test_all_mode_inside_a_branch_route_is_rejected() -> None:
    def route_builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(summarize_partitions)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="split",
                        by=lambda record: "left",
                        routes={"left": [Step(route_builder)]},
                    )
                ],
            )
        ],
    )
    with pytest.raises(UnsupportedDurableDeclarationError, match="after convergence"):
        compile_durable_plan(pipeline, params={})


def test_each_mode_inside_a_branch_route_is_supported() -> None:
    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="split",
                        by=lambda record: "left",
                        routes={"left": [Step(enrich, name="routed")]},
                    )
                ],
            )
        ],
    )
    plan = compile_durable_plan(pipeline, params={})
    routed = [node for node in plan.nodes if node.step.name == "routed"]
    assert len(routed) == 1
    assert routed[0].descriptor.kind == "each"


def test_removed_input_scope_argument_fails() -> None:
    with pytest.raises(TypeError):
        Step(enrich, input_scope="all")  # type: ignore[call-arg]


# -- execution ---------------------------------------------------------------


def test_each_preserves_partition_identity_through_execution(storage: Any) -> None:
    seen: list[str] = []

    def observe_partition(partition: PartitionInput, *, context: Any) -> Any:
        del context
        seen.append(partition.partition_id)
        return partition.load()

    def observe(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(observe_partition)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(observe)])])
    result = run(pipeline, job_id="each-1", source=source(), storage=storage)

    assert result.status == "completed"
    assert sorted(seen) == ["a", "b"]


def test_each_handle_exposes_partition_and_batch_identity(storage: Any) -> None:
    handles: list[tuple[str, str]] = []

    def capture(partition: PartitionInput, *, context: Any) -> Any:
        del context
        handles.append((partition.partition_id, partition.batch_id))
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(capture)

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

    def aggregate(partitions: AggregateInput, *, context: Any) -> Any:
        del context
        invocations.append(len(partitions))
        return {"count": len(partitions)}

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(aggregate)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, name="aggregate")]),
        ],
    )
    run(pipeline, job_id="all-2", source=source(), storage=storage)
    assert invocations == [2]


def test_callbacks_receive_runtime_context(storage: Any) -> None:
    observed: list[Any] = []

    def capture(partition: PartitionInput, *, context: Any) -> Any:
        observed.append(context)
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        assert context.step_name == "builder"
        return inputs.each(capture)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="ctx-1", source=source(), storage=storage, params={"mode": "x"})

    assert observed
    runtime = observed[0]
    assert runtime.step_name == "builder"
    assert runtime.run_id == "ctx-1"
    assert runtime.params == {"mode": "x"}
    assert hasattr(runtime, "run_root")


def test_ordinary_python_loops_are_supported_inside_callbacks(storage: Any) -> None:
    def multiply(partition: PartitionInput, *, context: Any) -> Any:
        del context
        payload = partition.load()
        total = 0
        for _ in range(3):
            total += int(payload["value"])
        return {"total": total}

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(multiply)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder, name="m")])])
    run(pipeline, job_id="loop-1", source=source(), storage=storage)

    assert storage.results(pipeline, job_id="loop-1").of("m")["a"] == {"total": 3}


def test_retry_operation_inside_an_each_callback(storage: Any) -> None:
    attempts: list[int] = []

    def flaky(partition: PartitionInput, *, context: Any) -> Any:
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

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(flaky)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(builder, name="flaky")])],
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

    def aggregate(partitions: AggregateInput, *, context: Any) -> Any:
        del context
        total = 0
        for partition in partitions.partitions():

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

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(aggregate)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, name="agg")]),
        ],
    )
    run(pipeline, job_id="retry-2", source=source(), storage=storage)

    assert storage.results(pipeline, job_id="retry-2").of("agg")["agg"] == {"total": 3}


def test_helper_retry_repeats_only_the_failed_partition(storage: Any) -> None:
    calls: dict[str, int] = {}

    def flaky(partition: PartitionInput, *, context: Any) -> Any:
        del context
        calls[partition.partition_id] = calls.get(partition.partition_id, 0) + 1
        if partition.partition_id == "b" and calls["b"] == 1:
            raise ConnectionError("transient")
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(flaky, retry=RetryOptions(max_attempts=3, delay_seconds=lambda _: 0.0))

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    result = run(pipeline, job_id="helper-retry", source=source(), storage=storage)

    assert result.status == "completed"
    assert calls == {"a": 1, "b": 2}


def test_managed_retry_inherits_controls_without_a_context_argument(storage: Any) -> None:
    """A nested operation retry needs no user-supplied execution context."""
    observed: list[bool] = []

    def inner(partition: PartitionInput, *, context: Any) -> Any:
        del context
        from caravel.retry import _active_retry_control

        observed.append(_active_retry_control.get() is not None)
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(inner)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="controls-1", source=source(), storage=storage)
    assert observed and all(observed)


def test_execution_control_is_reset_after_the_invocation(storage: Any) -> None:
    from caravel.retry import _active_retry_control

    run(two_stage_pipeline(), job_id="controls-2", source=source(), storage=storage)
    assert _active_retry_control.get() is None


def test_execution_control_is_reset_after_a_failure(storage: Any) -> None:
    from caravel.retry import _active_retry_control

    def boom(partition: PartitionInput, *, context: Any) -> Any:
        del context, partition
        raise RuntimeError("callback failed")

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(boom)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    with pytest.raises(RuntimeError):
        run(pipeline, job_id="controls-3", source=source(), storage=storage)
    assert _active_retry_control.get() is None


def test_committed_callbacks_are_not_repeated_on_resume(storage: Any, tmp_path: Path) -> None:
    calls: list[str] = []

    def once(partition: PartitionInput, *, context: Any) -> Any:
        del context
        calls.append(partition.partition_id)
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(once)

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(builder)])])
    run(pipeline, job_id="resume-1", source=source(), storage=storage)
    first = list(calls)
    run(pipeline, job_id="resume-1", source=source(), storage=storage)

    assert calls == first


def test_each_batching_preserves_durable_batch_identities(storage: Any) -> None:
    batches: list[str] = []

    def per_batch(partition: PartitionInput, *, context: Any) -> Any:
        del context
        batches.append(partition.batch_id)
        return partition.load()

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(per_batch)

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

    def aggregate(partitions: AggregateInput, *, context: Any) -> Any:
        del context
        sizes.append(len(partitions))
        return {"ids": sorted(p.partition_id for p in partitions.partitions())}

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(aggregate)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, name="agg", batch_size=1)]),
        ],
    )
    run(pipeline, job_id="batch-2", source=source(), storage=storage)
    assert sizes == [1, 1]


def test_empty_sealed_input_gives_an_empty_aggregate_reader(storage: Any) -> None:
    observed: list[bool] = []

    def aggregate(partitions: AggregateInput, *, context: Any) -> Any:
        del context
        observed.append(partitions.is_empty)
        return {"count": len(partitions)}

    def builder(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(aggregate)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich)]),
            Stage(name="combine", entries=[Step(builder, name="agg")]),
        ],
    )
    result = run(pipeline, job_id="empty-1", source=InMemorySource({}), storage=storage)
    assert result.status == "completed"
    assert observed == [True]


def test_upstream_failure_keeps_the_barrier_closed(storage: Any) -> None:
    reached: list[str] = []

    def failing(partition: PartitionInput, *, context: Any) -> Any:
        del context, partition
        raise RuntimeError("upstream failed")

    def upstream(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(failing)

    def aggregate(partitions: AggregateInput, *, context: Any) -> Any:
        del context, partitions
        reached.append("aggregate")
        return {}

    def downstream(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.all(aggregate)

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(upstream)]),
            Stage(name="combine", entries=[Step(downstream, name="agg")]),
        ],
    )
    with pytest.raises(RuntimeError):
        run(pipeline, job_id="closed-1", source=source(), storage=storage)
    assert reached == []


def test_incompatible_persisted_declaration_fails_before_mutation(storage: Any) -> None:
    """Changing the helper kind fails closed and asks for a new job id."""
    run(two_stage_pipeline(), job_id="fingerprint-1", source=source(), storage=storage)

    def as_each(inputs, *, context: Any) -> ManagedDescriptor:
        del context
        return inputs.each(enrich_partition)

    changed = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(enrich, output=JSONDataset())]),
            Stage(name="combine", entries=[Step(as_each, name="summarize")]),
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


def test_the_managed_input_surface_is_public() -> None:
    for name in (
        "Inputs",
        "ManagedDescriptor",
        "DeclarationContext",
        "PartitionInput",
        "AggregateInput",
        "ManagedInputError",
    ):
        assert hasattr(caravel, name), name
        assert name in caravel.__all__, name


def test_the_committed_dataset_reader_is_gone() -> None:
    """The all-input reader is AggregateInput now; the old name is not aliased."""
    import caravel.durability as durability

    assert not hasattr(durability, "CommittedDatasetReader")


def test_dictionary_retry_configuration_is_rejected_on_helpers() -> None:
    builder = Inputs()
    with pytest.raises(ManagedInputError, match="RetryOptions"):
        builder.each(enrich_partition, retry={"max_attempts": 2})  # type: ignore[arg-type]
