"""Acceptance tests for the ST-12 runtime input contract.

A processor is ordinary processing code. It receives one public ``StepInputs``
object and runtime context, in every scope, at every batch size. Scope is an
explicit declaration value that changes readiness and membership only: the
argument type, the handle type, and the handle interface are identical under
``input_scope="partition"`` and ``input_scope="all"``.

These tests are written against the public API. They assert type identity and
member sets rather than duck-typing a method call, because the contract is that
both scopes receive literally the same classes.
"""

from __future__ import annotations

import json
from functools import partial
from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import (
    Branch,
    DurableStorage,
    InputPartition,
    JSONDataset,
    Pipeline,
    RetryOptions,
    Stage,
    Step,
    StepInputs,
    retry_operation,
    run,
)
from caravel.durability import (
    AGGREGATE_PARTITION_ID,
    InMemorySource,
    InvalidTransitionError,
    UnsupportedDurableDeclarationError,
)
from caravel.durability.scheduler import compile_durable_plan, declaration_fingerprint

PIPELINE = "step-inputs"
DOCUMENTS = {"a": {"value": 1}, "b": {"value": 2}}


@pytest.fixture()
def storage(tmp_path: Path) -> Iterator[Any]:
    with DurableStorage.at(tmp_path / "state") as opened:
        yield opened


def source() -> InMemorySource:
    return InMemorySource(DOCUMENTS)


def _run(pipeline: Pipeline, storage: Any, *, job_id: str = "j1", **kwargs: Any) -> Any:
    return run(pipeline, job_id=job_id, source=source(), storage=storage, **kwargs)


# ---------------------------------------------------------------------------
# The identical processor under both scopes
# ---------------------------------------------------------------------------

OBSERVED: list[dict[str, Any]] = []


def scale(record: Any) -> Any:
    return {**record, "scaled": int(record["value"]) * 10}


def process(inputs, *, context):
    """One plain processor, used verbatim under both scopes."""
    OBSERVED.append(
        {
            "inputs_type": type(inputs),
            "inputs_members": frozenset(_public_members(inputs)),
            "batch_id": inputs.batch_id,
            "count": len(inputs),
            "is_empty": inputs.is_empty,
            "handle_types": tuple(type(handle) for handle in inputs.partitions()),
            "handle_members": tuple(
                frozenset(_public_members(handle)) for handle in inputs.partitions()
            ),
            "ids": tuple(handle.partition_id for handle in inputs.partitions()),
            "step": getattr(context, "step_name", None),
        }
    )
    results: dict[str, Any] = {}
    for handle in inputs.partitions():
        results[handle.partition_id] = scale(handle.load())
    return results


def _public_members(obj: Any) -> set[str]:
    return {name for name in dir(obj) if not name.startswith("_")}


def _observed_for(step: str) -> list[dict[str, Any]]:
    return [entry for entry in OBSERVED if entry["step"] == step]


@pytest.fixture(autouse=True)
def _reset_observations() -> Iterator[None]:
    OBSERVED.clear()
    yield
    OBSERVED.clear()


def _scoped_pipeline(name: str, scope: Any) -> Pipeline:
    """One step, one scope, reading the same source. Only the scope differs."""
    return Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(process, name=name, input_scope=scope)])],
    )


def test_the_same_processor_runs_under_both_scopes_with_one_input_class(storage: Any) -> None:
    """The discriminating test: one callable, two scopes, identical interfaces."""
    per_partition_pipeline = _scoped_pipeline("per_partition", "partition")
    aggregate_pipeline = _scoped_pipeline("aggregate", "all")

    assert _run(per_partition_pipeline, storage, job_id="each-1").status == "completed"
    assert _run(aggregate_pipeline, storage, job_id="all-1").status == "completed"

    per_partition = _observed_for("per_partition")
    aggregate = _observed_for("aggregate")
    assert len(per_partition) == 2, "partition scope runs once per source partition"
    assert len(aggregate) == 1, "all scope is one barrier work unit"

    # 1. The argument is the same class in both scopes.
    assert {entry["inputs_type"] for entry in per_partition + aggregate} == {StepInputs}
    # 2. Its public interface is identical in both scopes.
    assert len({entry["inputs_members"] for entry in per_partition + aggregate}) == 1
    # 3. Handles are the same class with the same interface in both scopes.
    handle_types = {
        handle_type for entry in per_partition + aggregate for handle_type in entry["handle_types"]
    }
    assert handle_types == {InputPartition}
    handle_members = {
        members for entry in per_partition + aggregate for members in entry["handle_members"]
    }
    assert len(handle_members) == 1
    assert handle_members == {frozenset({"partition_id", "batch_id", "count", "load"})}

    # 4. Only membership differs: one handle per unit vs. every upstream handle.
    assert sorted(entry["ids"] for entry in per_partition) == [("a",), ("b",)]
    assert aggregate[0]["ids"] == ("a", "b")

    # 5. Source identities are preserved unchanged in both scopes, and the same
    #    processor body produces the same per-partition results either way.
    each_results = storage.results(per_partition_pipeline, job_id="each-1")
    all_results = storage.results(aggregate_pipeline, job_id="all-1")
    assert each_results.of("per_partition") == {
        "a": {"a": {"value": 1, "scaled": 10}},
        "b": {"b": {"value": 2, "scaled": 20}},
    }
    assert all_results.of("aggregate") == {
        "aggregate": {
            "a": {"value": 1, "scaled": 10},
            "b": {"value": 2, "scaled": 20},
        }
    }


def test_step_inputs_exposes_exactly_the_declared_interface(storage: Any) -> None:
    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(process)])])
    assert _run(pipeline, storage).status == "completed"

    members = _observed_for("process")[0]["inputs_members"]
    assert members == frozenset({"batch_id", "partitions", "is_empty"})


def test_partitions_is_a_repeatable_lazy_iterator(storage: Any) -> None:
    seen: dict[str, Any] = {}

    def inspect(inputs, *, context):
        del context
        first = inputs.partitions()
        second = inputs.partitions()
        seen["iterator"] = iter(first) is first
        seen["distinct"] = first is not second
        seen["first_ids"] = [handle.partition_id for handle in first]
        seen["second_ids"] = [handle.partition_id for handle in second]
        seen["counts"] = [handle.count for handle in inputs.partitions()]
        return {"ok": True}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(inspect, input_scope="all")])],
    )
    assert _run(pipeline, storage).status == "completed"

    assert seen["iterator"] is True
    assert seen["distinct"] is True
    assert seen["first_ids"] == seen["second_ids"] == ["a", "b"]
    assert seen["counts"] == [1, 1]


def test_a_handle_loads_its_payload_only_when_asked(storage: Any) -> None:
    loaded: list[str] = []

    def peek(inputs, *, context):
        del context
        handles = list(inputs.partitions())
        assert loaded == [], "constructing StepInputs must not load any payload"
        for handle in handles:
            loaded.append(handle.partition_id)
            handle.load()
        return {"seen": len(handles)}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(peek, input_scope="all")])],
    )
    assert _run(pipeline, storage).status == "completed"
    assert loaded == ["a", "b"]


# ---------------------------------------------------------------------------
# Compilation never runs user code
# ---------------------------------------------------------------------------


def test_compilation_never_invokes_a_processor() -> None:
    calls: list[str] = []

    def explode(inputs, *, context):  # pragma: no cover - must never run
        calls.append("called")
        raise AssertionError("compilation invoked the processor")

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="s", entries=[Step(explode), Step(explode, input_scope="all")]),
        ],
    )
    plan = compile_durable_plan(pipeline)
    assert calls == []
    assert [node.input_scope for node in plan.nodes] == ["partition", "all"]


def test_compilation_reads_scope_from_the_declaration_not_from_the_callable() -> None:
    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(process, name="one"),
                    Step(process, name="two", input_scope="all"),
                ],
            )
        ],
    )
    plan = compile_durable_plan(pipeline)
    assert [(node.step.name, node.input_scope) for node in plan.nodes] == [
        ("one", "partition"),
        ("two", "all"),
    ]
    assert OBSERVED == []


def test_compilation_never_reads_the_source(storage: Any) -> None:
    class RecordingSource(InMemorySource):
        def __init__(self) -> None:
            super().__init__(DOCUMENTS)
            self.reads: list[str] = []

        def discover(self) -> Any:
            self.reads.append("discover")
            return super().discover()

        def load(self, record: Any) -> Any:
            self.reads.append(f"load:{record.partition_id}")
            return super().load(record)

    recording = RecordingSource()
    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(process)])])
    compile_durable_plan(pipeline)
    assert recording.reads == []
    assert run(pipeline, job_id="j1", source=recording, storage=storage).status == "completed"
    assert "discover" in recording.reads


# ---------------------------------------------------------------------------
# Declaration validation
# ---------------------------------------------------------------------------


def test_an_invalid_scope_is_rejected_with_an_actionable_error() -> None:
    with pytest.raises(ValueError, match="input_scope"):
        Step(process, input_scope="everything")  # type: ignore[arg-type]


def test_a_processor_must_accept_one_input_and_a_context_keyword() -> None:
    with pytest.raises(TypeError, match="context"):
        Step(lambda inputs: inputs)

    with pytest.raises(TypeError, match="context"):
        Step(lambda: None)


def test_partials_and_callable_objects_are_supported(storage: Any) -> None:
    def multiply(inputs, *, context, factor: int):
        del context
        return {
            handle.partition_id: int(handle.load()["value"]) * factor
            for handle in inputs.partitions()
        }

    class Doubler:
        def __call__(self, inputs, *, context):
            del context
            return {
                handle.partition_id: int(handle.load()["value"]) * 2
                for handle in inputs.partitions()
            }

    tripled = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(partial(multiply, factor=3), name="tripled")])],
    )
    doubled = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(Doubler(), name="doubled", input_scope="all")])],
    )
    assert _run(tripled, storage, job_id="partial-1").status == "completed"
    assert _run(doubled, storage, job_id="object-1").status == "completed"
    assert storage.results(tripled, job_id="partial-1").of("tripled") == {
        "a": {"a": 3},
        "b": {"b": 6},
    }
    assert storage.results(doubled, job_id="object-1").of("doubled") == {
        "doubled": {"a": 2, "b": 4}
    }


def test_runtime_context_and_params_reach_the_processor(storage: Any) -> None:
    captured: dict[str, Any] = {}

    def read_context(inputs, *, context):
        captured["params"] = dict(context.params)
        captured["step_name"] = context.step_name
        captured["stage_name"] = context.stage_name
        return {"ok": True}

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(read_context)])])
    assert _run(pipeline, storage, params={"mode": "fast"}).status == "completed"
    assert captured["params"] == {"mode": "fast"}
    assert captured["step_name"] == "read_context"
    assert captured["stage_name"] == "s"


def test_a_context_factory_still_wraps_the_framework_context(storage: Any) -> None:
    seen: dict[str, Any] = {}

    class AppContext:
        def __init__(self, base: Any) -> None:
            self.base = base
            self.service = "search"

    def read_app_context(inputs, *, context):
        seen["service"] = context.service
        seen["step_name"] = context.base.step_name
        return {"ok": True}

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(read_app_context)])])
    assert _run(pipeline, storage, context_factory=AppContext).status == "completed"
    assert seen == {"service": "search", "step_name": "read_app_context"}


# ---------------------------------------------------------------------------
# Removed builder API
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "removed",
    [
        "Inputs",
        "ManagedDescriptor",
        "DeclarationContext",
        "PartitionInput",
        "AggregateInput",
        "ManagedInputError",
    ],
)
def test_the_builder_api_is_gone(removed: str) -> None:
    assert not hasattr(caravel, removed)
    assert removed not in caravel.__all__


# ---------------------------------------------------------------------------
# Scope changes readiness and membership
# ---------------------------------------------------------------------------


def test_an_all_scope_step_waits_for_every_upstream_commit(storage: Any) -> None:
    order: list[str] = []

    def stamp(inputs, *, context):
        [handle] = inputs.partitions()
        order.append(f"{context.step_name}:{handle.partition_id}")
        return handle.load()

    def summarize(inputs, *, context):
        order.append(f"{context.step_name}:{len(inputs)}")
        return {"documents": sorted(handle.partition_id for handle in inputs.partitions())}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(name="prepare", entries=[Step(stamp)]),
            Stage(name="finish", entries=[Step(summarize, input_scope="all")]),
        ],
    )
    assert _run(pipeline, storage).status == "completed"
    assert order[-1] == "summarize:2"
    assert sorted(order[:-1]) == ["stamp:a", "stamp:b"]
    assert storage.results(pipeline, job_id="j1").of("summarize") == {
        "summarize": {"documents": ["a", "b"]}
    }


def test_an_empty_sealed_all_input_scope_exposes_zero_handles(storage: Any) -> None:
    calls: list[dict[str, Any]] = []

    def summarize(inputs, *, context):
        del context
        calls.append({"count": len(inputs), "empty": inputs.is_empty})
        return {"documents": [handle.partition_id for handle in inputs.partitions()]}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="finish", entries=[Step(summarize, input_scope="all")])],
    )
    result = run(
        pipeline,
        job_id="empty-1",
        source=InMemorySource({}),
        storage=storage,
    )
    assert result.status == "completed"
    assert calls == [{"count": 0, "empty": True}]


def test_an_empty_list_payload_is_still_one_partition_handle(storage: Any) -> None:
    seen: dict[str, Any] = {}

    def observe(inputs, *, context):
        del context
        handles = list(inputs.partitions())
        seen["count"] = len(handles)
        seen["payload"] = handles[0].load()
        return {"records": handles[0].load()}

    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(observe)])])
    assert (
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"a": []}),
            storage=storage,
        ).status
        == "completed"
    )
    assert seen == {"count": 1, "payload": []}


def test_an_all_scope_step_after_a_barrier_reads_the_logical_upstream_result(
    storage: Any,
) -> None:
    observed: list[tuple[str, ...]] = []

    def first_summary(inputs, *, context):
        del context
        return {"ids": sorted(handle.partition_id for handle in inputs.partitions())}

    def second_summary(inputs, *, context):
        del context
        handles = list(inputs.partitions())
        observed.append(tuple(handle.partition_id for handle in handles))
        return {"upstream": [handle.load() for handle in handles]}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="roll-up",
                entries=[
                    Step(first_summary, input_scope="all", output=JSONDataset(name="summary")),
                    Step(second_summary, input_scope="all", output=JSONDataset(name="recap")),
                ],
            )
        ],
    )
    assert _run(pipeline, storage).status == "completed"

    assert observed == [("summary",)], "a private aggregate id must never reach user keys"
    assert AGGREGATE_PARTITION_ID not in json.dumps(
        storage.results(pipeline, job_id="j1").of("second_summary")
    )
    assert storage.results(pipeline, job_id="j1").of("second_summary") == {
        "recap": {"upstream": [{"ids": ["a", "b"]}]}
    }


def test_a_partition_scope_step_after_a_barrier_sees_the_logical_name(storage: Any) -> None:
    observed: list[str] = []

    def summarize(inputs, *, context):
        del context
        return {"ids": sorted(handle.partition_id for handle in inputs.partitions())}

    def restate(inputs, *, context):
        del context
        [handle] = inputs.partitions()
        observed.append(handle.partition_id)
        return {"restated": handle.load()}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(summarize, input_scope="all", output=JSONDataset(name="summary")),
                    Step(restate, output=JSONDataset(name="restated")),
                ],
            )
        ],
    )
    assert _run(pipeline, storage).status == "completed"
    assert observed == ["summary"]


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------


def test_partition_scope_batching_exposes_one_handle_per_batch(storage: Any) -> None:
    batches: list[tuple[str, Any]] = []

    def chunk(inputs, *, context):
        del context
        [handle] = inputs.partitions()
        payload = handle.load()
        batches.append((inputs.batch_id, payload))
        assert handle.batch_id == inputs.batch_id
        assert handle.count == len(payload)
        return payload

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(chunk, batch_size=2)])],
    )
    assert (
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"a": [1, 2, 3, 4, 5]}),
            storage=storage,
        ).status
        == "completed"
    )
    assert batches == [
        ("batch-000000", [1, 2]),
        ("batch-000001", [3, 4]),
        ("batch-000002", [5]),
    ]


def test_all_scope_batching_slices_upstream_handles(storage: Any) -> None:
    batches: list[tuple[str, tuple[str, ...]]] = []

    def summarize(inputs, *, context):
        del context
        ids = tuple(handle.partition_id for handle in inputs.partitions())
        batches.append((inputs.batch_id, ids))
        assert all(handle.batch_id == inputs.batch_id for handle in inputs.partitions())
        return {"ids": list(ids)}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(summarize, input_scope="all", batch_size=1)])],
    )
    assert (
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"a": {"value": 1}, "b": {"value": 2}, "c": {"value": 3}}),
            storage=storage,
        ).status
        == "completed"
    )
    assert batches == [
        ("batch-000000", ("a",)),
        ("batch-000001", ("b",)),
        ("batch-000002", ("c",)),
    ]


def test_the_input_class_is_identical_across_batch_sizes(storage: Any) -> None:
    types_seen: set[Any] = set()
    members_seen: set[frozenset[str]] = set()

    def observe(inputs, *, context):
        del context
        types_seen.add(type(inputs))
        members_seen.add(frozenset(_public_members(inputs)))
        for handle in inputs.partitions():
            types_seen.add(type(handle))
        return [handle.load() for handle in inputs.partitions()]

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(observe, name="unbatched"),
                    Step(observe, name="batched", batch_size=1),
                ],
            )
        ],
    )
    assert (
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"a": [1, 2, 3]}),
            storage=storage,
        ).status
        == "completed"
    )
    assert types_seen == {StepInputs, InputPartition}
    assert len(members_seen) == 1


# ---------------------------------------------------------------------------
# Branch routing
# ---------------------------------------------------------------------------


def test_route_processors_receive_step_inputs(storage: Any) -> None:
    observed: dict[str, Any] = {}

    def tag(inputs, *, context):
        del context
        [handle] = inputs.partitions()
        observed[handle.partition_id] = (type(inputs), type(handle))
        record: Any = handle.load()
        return {**record, "tagged": True}

    def finish(inputs, *, context):
        del context
        [handle] = inputs.partitions()
        return handle.load()

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="by_kind",
                        by=lambda record: str(record["kind"]),
                        routes={"x": [Step(tag)], "y": [Step(tag)]},
                    ),
                    Step(finish),
                ],
            )
        ],
    )
    result = run(
        pipeline,
        job_id="j1",
        source=InMemorySource({"a": {"kind": "x"}, "b": {"kind": "y"}}),
        storage=storage,
    )
    assert result.status == "completed"
    assert observed == {
        "a": (StepInputs, InputPartition),
        "b": (StepInputs, InputPartition),
    }


def test_a_route_step_cannot_declare_all_scope() -> None:
    def summarize(inputs, *, context):  # pragma: no cover - rejected at compile
        del context
        return {"documents": len(inputs)}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="by_kind",
                        by=lambda record: "x",
                        routes={"x": [Step(summarize, input_scope="all")]},
                    )
                ],
            )
        ],
    )
    with pytest.raises(UnsupportedDurableDeclarationError, match="after convergence"):
        compile_durable_plan(pipeline)


def test_a_barrier_after_a_branch_sees_every_routed_partition(storage: Any) -> None:
    def tag(inputs, *, context):
        del context
        [handle] = inputs.partitions()
        record: Any = handle.load()
        return {**record, "tagged": True}

    def summarize(inputs, *, context):
        del context
        return {"ids": sorted(handle.partition_id for handle in inputs.partitions())}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="by_kind",
                        by=lambda record: str(record["kind"]),
                        routes={"x": [Step(tag)], "y": [Step(tag)]},
                    )
                ],
            ),
            Stage(name="finish", entries=[Step(summarize, input_scope="all")]),
        ],
    )
    assert (
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"a": {"kind": "x"}, "b": {"kind": "y"}}),
            storage=storage,
        ).status
        == "completed"
    )
    assert storage.results(pipeline, job_id="j1").of("summarize") == {
        "summarize": {"ids": ["a", "b"]}
    }


# ---------------------------------------------------------------------------
# Retries stay the one shared contract
# ---------------------------------------------------------------------------


def test_an_explicit_retry_operation_inside_a_processor_targets_one_partition(
    storage: Any,
) -> None:
    attempts: dict[str, int] = {}

    def flaky(partition_id: str, payload: Any) -> Any:
        attempts[partition_id] = attempts.get(partition_id, 0) + 1
        if attempts[partition_id] < 2:
            raise RuntimeError("transient")
        return {**payload, "ok": True}

    def process_each(inputs, *, context):
        del context
        results = {}
        for handle in inputs.partitions():
            value, _ = retry_operation(
                operation=partial(flaky, handle.partition_id, handle.load()),
                retry=RetryOptions(
                    max_attempts=3,
                    delay_seconds=lambda attempt: 0.0,
                    should_retry_error=lambda error: isinstance(error, RuntimeError),
                ),
            )
            results[handle.partition_id] = value
        return results

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(process_each, input_scope="all")])],
    )
    assert _run(pipeline, storage).status == "completed"
    assert attempts == {"a": 2, "b": 2}


def test_a_plain_loop_iteration_gets_no_hidden_retry(storage: Any) -> None:
    seen: list[str] = []

    def process_each(inputs, *, context):
        del context
        for handle in inputs.partitions():
            seen.append(handle.partition_id)
            raise RuntimeError("boom")
        return {}

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(process_each, input_scope="all")])],
    )
    with pytest.raises(RuntimeError, match="boom"):
        _run(pipeline, storage)
    assert seen == ["a"], "an ordinary loop iteration acquires no retry of its own"


def test_step_retry_skips_batches_already_committed(storage: Any) -> None:
    attempts: list[str] = []

    def chunk(inputs, *, context):
        del context
        [handle] = inputs.partitions()
        attempts.append(inputs.batch_id)
        if inputs.batch_id == "batch-000001" and attempts.count("batch-000001") == 1:
            raise RuntimeError("transient")
        return handle.load()

    pipeline = Pipeline(
        name=PIPELINE,
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(
                        chunk,
                        batch_size=1,
                        retry=RetryOptions(
                            max_attempts=3,
                            delay_seconds=lambda attempt: 0.0,
                            should_retry_error=lambda error: isinstance(error, RuntimeError),
                        ),
                    )
                ],
            )
        ],
    )
    assert (
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource({"a": [1, 2, 3]}),
            storage=storage,
        ).status
        == "completed"
    )
    assert attempts == [
        "batch-000000",
        "batch-000001",
        "batch-000001",
        "batch-000002",
    ], "the retry resumes at the failed batch instead of republishing committed ones"


# ---------------------------------------------------------------------------
# Declaration fingerprint
# ---------------------------------------------------------------------------


def test_the_declaration_fingerprint_is_version_three() -> None:
    pipeline = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(process)])])
    assert declaration_fingerprint(compile_durable_plan(pipeline)).startswith("schema=3|")


def test_scope_is_part_of_the_fingerprint() -> None:
    def build(scope: Any) -> Pipeline:
        return Pipeline(
            name=PIPELINE,
            stages=[Stage(name="s", entries=[Step(process, input_scope=scope)])],
        )

    each = declaration_fingerprint(compile_durable_plan(build("partition")))
    every = declaration_fingerprint(compile_durable_plan(build("all")))
    assert each != every


def test_a_job_from_an_older_fingerprint_is_rejected_before_any_source_read(
    storage: Any,
) -> None:
    class RecordingSource(InMemorySource):
        def __init__(self) -> None:
            super().__init__(DOCUMENTS)
            self.reads: list[str] = []

        def discover(self) -> Any:
            self.reads.append("discover")
            return super().discover()

    def summarize(inputs, *, context):
        del context
        return {"documents": len(inputs)}

    first = Pipeline(name=PIPELINE, stages=[Stage(name="s", entries=[Step(process)])])
    assert _run(first, storage).status == "completed"

    changed = Pipeline(
        name=PIPELINE,
        stages=[Stage(name="s", entries=[Step(summarize, name="process", input_scope="all")])],
    )
    recording = RecordingSource()
    with pytest.raises(InvalidTransitionError, match="fingerprint"):
        run(changed, job_id="j1", source=recording, storage=storage)
    assert recording.reads == [], "a stale declaration must fail before the source is consulted"
