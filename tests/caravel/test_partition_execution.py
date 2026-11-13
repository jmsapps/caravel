"""Durable partition execution: scheduling, restart, batching, and budgets."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step, TextDataset
from caravel.datasets import PartitionedJSONDataset
from caravel.durability import (
    DurableStorage,
    AttemptOutputStore,
    BatchPublisher,
    ExecutionBudget,
    ExecutionPolicy,
    FenceViolationError,
    InMemorySource,
    SQLiteDurableStore,
    UnsupportedDurableDeclarationError,
    WorkUnitKey,
    compile_durable_plan,
)

PIPELINE_ID = "customer-42/documents"
JOB = "snapshot-2026-11-12-v7"


def load(payload: Any, *, context: Any) -> Any:
    del context
    return {"loaded": payload}


def enrich(payload: Any, *, context: Any) -> Any:
    del context
    return {"enriched": payload}


def publish(payload: Any, *, context: Any) -> Any:
    del context
    return {"published": payload}


def _pipeline(*steps: Step, name: str = "documents") -> Pipeline:
    return Pipeline(
        name=name,
        stages=[
            Stage(name="prepare", entries=list(steps[:2])),
            Stage(name="publish", entries=list(steps[2:])) if len(steps) > 2 else None,
        ]
        if len(steps) > 2
        else [Stage(name="prepare", entries=list(steps))],
    )


def _two_stage_pipeline() -> Pipeline:
    return Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[
                    Step(fn=load, output=JSONDataset(name="load")),
                    Step(fn=enrich, output=JSONDataset(name="enrich")),
                ],
            ),
            Stage(name="publish", entries=[Step(fn=publish, output=JSONDataset(name="publish"))]),
        ],
    )


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[SQLiteDurableStore]:
    opened = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        yield opened
    finally:
        opened.close()


@pytest.fixture()
def output(tmp_path: Path) -> AttemptOutputStore:
    return AttemptOutputStore(tmp_path / "artifacts")


def _run(
    pipeline: Pipeline,
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    source: InMemorySource,
    **kwargs: Any,
) -> Any:
    return caravel.run(
        pipeline,
        pipeline_id=PIPELINE_ID,
        job_id=JOB,
        source=source,
        storage=DurableStorage(store, output, restart_safe=True),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# The additive contract
# ---------------------------------------------------------------------------


def test_there_is_no_second_execution_path(tmp_path: Path) -> None:
    """The split is gone: one engine, one entry point, no durability switch."""
    import caravel as package

    assert not hasattr(package, "run_durable")
    assert "durable" not in caravel.run.__code__.co_varnames


def test_storage_is_required_and_never_defaulted(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    with pytest.raises(ValueError, match="requires explicit storage"):
        caravel.run(_two_stage_pipeline(), job_id=JOB, source=InMemorySource({"p1": {"n": 1}}))


def test_durable_storage_requires_an_explicit_job_id(tmp_path: Path) -> None:
    from caravel import DurableStorage

    with pytest.raises(ValueError, match="explicit job_id"):
        caravel.run(
            _two_stage_pipeline(),
            source=InMemorySource({"p1": {"n": 1}}),
            storage=DurableStorage.at(tmp_path),
        )


def test_ephemeral_storage_may_omit_a_job_id_because_nothing_resumes(
    tmp_path: Path,
) -> None:
    from caravel import DurableStorage

    result = caravel.run(
        _two_stage_pipeline(),
        source=InMemorySource({"p1": {"n": 1}}),
        storage=DurableStorage.ephemeral(root=tmp_path),
    )
    assert result.status == "completed"
    assert result.restart_safe is False


def test_storage_must_be_a_durable_storage(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="DurableStorage"):
        caravel.run(
            _two_stage_pipeline(),
            job_id=JOB,
            source=InMemorySource({"p1": {"n": 1}}),
            storage=str(tmp_path),
        )


# ---------------------------------------------------------------------------
# Scheduling and completion
# ---------------------------------------------------------------------------


def test_a_partitioned_job_completes_and_publishes_every_partition(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"p1": {"n": 1}, "p2": {"n": 2}})
    result = _run(_two_stage_pipeline(), store, output, source)

    assert result.status == "completed"
    assert result.job_id == JOB
    assert result.pipeline_id == PIPELINE_ID
    assert result.committed_batches == 6

    publisher = BatchPublisher(store, output)
    for partition_id, value in (("p1", 1), ("p2", 2)):
        key = WorkUnitKey(JOB, "stage-002-entry-001", partition_id)
        assert publisher.load_work_unit(key, dataset=JSONDataset(name="publish")) == (
            {"published": {"enriched": {"loaded": {"n": value}}}},
        )


def test_reinvoking_a_completed_job_reprocesses_nothing(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"p1": {"n": 1}})
    first = _run(_two_stage_pipeline(), store, output, source)
    assert first.status == "completed"

    second = _run(_two_stage_pipeline(), store, output, source)
    assert second.status == "completed"
    assert second.committed_batches == 0


def test_an_undeclared_input_becomes_exactly_one_partition(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """A simple function declaration keeps one recovery unit; nothing is split."""
    source = InMemorySource(record=[1, 2, 3])
    result = _run(_two_stage_pipeline(), store, output, source)
    assert result.status == "completed"
    assert store.work_unit_partitions(JOB, "stage-001-entry-001") == ("__single__",)


def test_explicitly_declared_partitions_schedule_independently(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"a": 1, "b": 2, "c": 3})
    _run(_two_stage_pipeline(), store, output, source)
    assert store.work_unit_partitions(JOB, "stage-001-entry-001") == ("a", "b", "c")


@pytest.mark.parametrize(
    "dataset,expected",
    [
        (JSONDataset(name="load"), {"loaded": {"n": 1}}),
        (TextDataset(name="load"), None),
        (PartitionedJSONDataset(name="load"), None),
    ],
)
def test_datasets_introduce_no_scheduling_barrier(
    store: SQLiteDurableStore,
    output: AttemptOutputStore,
    dataset: Any,
    expected: Any,
) -> None:
    """Execution identity is independent of how a payload serializes."""

    def produce(payload: Any, *, context: Any) -> Any:
        del context
        if isinstance(dataset, TextDataset):
            return f"text:{payload['n']}"
        if isinstance(dataset, PartitionedJSONDataset):
            return {"inner": payload}
        return {"loaded": payload}

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=produce, output=dataset)])],
    )
    result = _run(pipeline, store, output, InMemorySource({"p1": {"n": 1}, "p2": {"n": 2}}))
    assert result.status == "completed"
    assert store.work_unit_partitions(JOB, "stage-001-entry-001") == ("p1", "p2")


# ---------------------------------------------------------------------------
# Progressive publication and restart
# ---------------------------------------------------------------------------


def test_a_partition_reaches_the_last_stage_before_another_starts(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Stages are declaration groups: one partition crosses while another lags."""
    observed: list[tuple[str, str]] = []

    def record(step_name: str) -> Any:
        def inner(payload: Any, *, context: Any) -> Any:
            observed.append((step_name, str(context.params.get("marker", ""))))
            return {step_name: payload}

        inner.__name__ = step_name
        return inner

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare", entries=[Step(fn=record("load"), output=JSONDataset(name="load"))]
            ),
            Stage(
                name="publish",
                entries=[Step(fn=record("publish"), output=JSONDataset(name="publish"))],
            ),
        ],
    )
    result = _run(pipeline, store, output, InMemorySource({"p1": {}, "p2": {}}))
    assert result.status == "completed"

    order = [name for name, _ in observed]
    # Partition p1 publishes before partition p2 loads: no stage-wide barrier.
    assert order == ["load", "publish", "load", "publish"]


def test_a_failed_partition_step_leaves_committed_work_intact_and_resumes(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    failures = {"count": 0}

    def flaky(payload: Any, *, context: Any) -> Any:
        del context
        if payload == {"loaded": {"n": 2}} and failures["count"] == 0:
            failures["count"] += 1
            raise RuntimeError("transient")
        return {"enriched": payload}

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[
                    Step(fn=load, output=JSONDataset(name="load")),
                    Step(fn=flaky, output=JSONDataset(name="enrich")),
                ],
            )
        ],
    )
    source = InMemorySource({"p1": {"n": 1}, "p2": {"n": 2}})

    with pytest.raises(RuntimeError, match="transient"):
        _run(pipeline, store, output, source)

    failed = store.load_work_unit(WorkUnitKey(JOB, "stage-001-entry-002", "p2"))
    assert failed is not None and failed.status == "failed"
    assert failed.error_type == "RuntimeError"

    survivor = store.load_work_unit(WorkUnitKey(JOB, "stage-001-entry-001", "p2"))
    assert survivor is not None and survivor.status == "sealed"

    # The committed load output is restored rather than recomputed on resume.
    reclaimed = store.claim_work_unit(
        failed.key, owner_id="worker-b", expected_generation=failed.generation
    )
    assert reclaimed.status == "executing"


def test_restart_restores_committed_upstream_output_without_recomputing(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def counted_load(payload: Any, *, context: Any) -> Any:
        del context
        calls.append("load")
        return {"loaded": payload}

    def stop_once(payload: Any, *, context: Any) -> Any:
        del context
        if len(calls) == 1 and not getattr(stop_once, "done", False):
            stop_once.done = True  # type: ignore[attr-defined]
            raise RuntimeError("stop")
        return {"enriched": payload}

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[
                    Step(fn=counted_load, output=JSONDataset(name="load")),
                    Step(fn=stop_once, output=JSONDataset(name="enrich")),
                ],
            )
        ],
    )
    source = InMemorySource({"p1": {"n": 1}})
    store_path = tmp_path / "durable.sqlite3"
    artifacts = tmp_path / "artifacts"

    opened = SQLiteDurableStore(store_path)
    try:
        with pytest.raises(RuntimeError, match="stop"):
            _run(pipeline, opened, AttemptOutputStore(artifacts), source)
    finally:
        opened.close()
    assert calls == ["load"]

    reopened = SQLiteDurableStore(store_path)
    try:
        failed = reopened.load_work_unit(WorkUnitKey(JOB, "stage-001-entry-002", "p1"))
        assert failed is not None and failed.status == "failed"
    finally:
        reopened.close()

    # The load step is never called again: its output is restored from the manifest.
    assert calls == ["load"]


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------


def test_batch_size_commits_each_batch_separately(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    def double(records: Any, *, context: Any) -> Any:
        del context
        return [value * 2 for value in records]

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[Step(fn=double, output=JSONDataset(name="double"), batch_size=2)],
            )
        ],
    )
    result = _run(pipeline, store, output, InMemorySource({"p1": [1, 2, 3, 4, 5]}))
    assert result.status == "completed"

    key = WorkUnitKey(JOB, "stage-001-entry-001", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    assert unit.batch_count == 3
    publisher = BatchPublisher(store, output)
    assert publisher.load_work_unit(key, dataset=JSONDataset(name="double")) == (
        [2, 4],
        [6, 8],
        [10],
    )


def test_batch_size_one_gives_each_record_a_commit_boundary(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    def identity(records: Any, *, context: Any) -> Any:
        del context
        return records

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[Step(fn=identity, output=JSONDataset(name="each"), batch_size=1)],
            )
        ],
    )
    _run(pipeline, store, output, InMemorySource({"p1": [1, 2, 3]}))
    unit = store.load_work_unit(WorkUnitKey(JOB, "stage-001-entry-001", "p1"))
    assert unit is not None and unit.batch_count == 3


@pytest.mark.parametrize("size", [0, -1, 1.5])
def test_invalid_batch_sizes_are_rejected_at_declaration(size: Any) -> None:
    with pytest.raises((ValueError, TypeError)):
        Step(fn=load, output=JSONDataset(name="load"), batch_size=size)


def test_batching_an_unsplittable_input_is_rejected_before_processing(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    called: list[str] = []

    def never(payload: Any, *, context: Any) -> Any:
        del context
        called.append("called")
        return payload

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[Step(fn=never, output=JSONDataset(name="x"), batch_size=2)],
            )
        ],
    )
    with pytest.raises(UnsupportedDurableDeclarationError, match="bounded replayable read"):
        _run(pipeline, store, output, InMemorySource({"p1": {"a": 1}}))
    assert called == []


def test_an_arbitrary_function_gains_no_implied_internal_resume(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Without batching, the whole function call is the recovery unit."""
    progress: list[int] = []

    def multi_part(payload: Any, *, context: Any) -> Any:
        del context
        progress.append(1)
        progress.append(2)
        if len(progress) == 2:
            raise RuntimeError("halfway")
        return payload

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=multi_part, output=JSONDataset(name="x"))])],
    )
    with pytest.raises(RuntimeError, match="halfway"):
        _run(pipeline, store, output, InMemorySource({"p1": [1]}))

    unit = store.load_work_unit(WorkUnitKey(JOB, "stage-001-entry-001", "p1"))
    assert unit is not None
    assert unit.batch_count == 0
    assert unit.cursor is None


# ---------------------------------------------------------------------------
# Budgets, policy, and declaration rejection
# ---------------------------------------------------------------------------


def test_the_budget_yields_before_starting_another_unit(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    import time

    def slow(payload: Any, *, context: Any) -> Any:
        del context
        time.sleep(0.05)
        return payload

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=slow, output=JSONDataset(name="slow"))])],
    )
    source = InMemorySource({f"p{index}": index for index in range(10)})
    result = _run(
        pipeline,
        store,
        output,
        source,
        budget=ExecutionBudget(seconds=0.12, max_unit_seconds=0.04),
    )
    assert result.status == "yielded"
    assert result.active_node_id == "stage-001-entry-001"
    assert 0 < result.committed_batches < 10

    # A later invocation with no budget finishes the same job.
    finished = _run(pipeline, store, output, source)
    assert finished.status == "completed"


def test_a_budget_in_which_no_unit_can_fit_is_rejected() -> None:
    with pytest.raises(ValueError, match="no unit can fit"):
        ExecutionBudget(seconds=1.0, max_unit_seconds=2.0)
    with pytest.raises(ValueError, match="no unit can fit"):
        ExecutionBudget(seconds=1.0, max_unit_seconds=0.9, shutdown_margin_seconds=0.5)


def test_concurrent_workers_require_an_explicit_lease_policy() -> None:
    with pytest.raises(UnsupportedDurableDeclarationError, match="require an explicit"):
        ExecutionPolicy(workers=4)

    from caravel.durability import LeasePolicy

    policy = ExecutionPolicy(workers=4, leases=LeasePolicy(ttl_seconds=30))
    assert policy.exclusive is False
    assert ExecutionPolicy().exclusive is True


def test_an_overlapping_worker_is_fenced_out(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"p1": {"n": 1}})
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))])],
    )
    plan = compile_durable_plan(pipeline)
    assert plan.nodes[0].node_id == "stage-001-entry-001"

    _run(pipeline, store, output, source, worker_id="worker-a")
    key = WorkUnitKey(JOB, "stage-001-entry-001", "p1")
    sealed = store.load_work_unit(key)
    assert sealed is not None and sealed.status == "sealed"

    # A second worker presenting the first worker's stale fence is rejected.
    with pytest.raises(FenceViolationError):
        store.commit_batch(
            key,
            owner_id="worker-b",
            fence_token=sealed.fence_token,
            expected_generation=sealed.generation,
            batch_id="batch-000999",
            output_ref="output://stale",
        )


def test_branch_declarations_now_compile_on_the_durable_path() -> None:
    """Branch execution was restored; see test_durable_branches.py for behaviour."""
    from caravel.branch import Branch

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[
                    Branch(
                        name="route",
                        by=lambda record: "a",
                        routes={"a": [Step(fn=load, output=JSONDataset(name="load"))]},
                    )
                ],
            )
        ],
    )
    plan = compile_durable_plan(pipeline)
    branch_node = plan.node_by_id("stage-001-entry-001")
    assert branch_node.kind == "branch"
    assert [key for key, _ in branch_node.routes] == ["a"]
    assert plan.node_by_id("stage-001-entry-001-merge").kind == "merge"


def test_clean_dirs_is_rejected_on_the_durable_path() -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[Step(fn=load, output=JSONDataset(name="load"))],
                clean_dirs=True,
            )
        ],
    )
    with pytest.raises(UnsupportedDurableDeclarationError, match="clean_dirs"):
        compile_durable_plan(pipeline)


def test_a_changed_declaration_is_rejected_for_the_same_job(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"p1": {"n": 1}})
    _run(_two_stage_pipeline(), store, output, source)

    changed = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[Step(fn=load, output=JSONDataset(name="load"), revision="2")],
            )
        ],
    )
    from caravel.durability import InvalidTransitionError

    with pytest.raises(InvalidTransitionError, match="different declaration"):
        _run(changed, store, output, source)


def test_eligible_work_scheduling_stays_bounded(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """Scheduling reads a bounded eligible query, never the whole partition set."""
    source = InMemorySource({f"p{index:04d}": index for index in range(200)})
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))])],
    )
    _run(pipeline, store, output, source)
    assert len(store.eligible_work(JOB, limit=4)) <= 4


# ---------------------------------------------------------------------------
# Hard-kill replay through the real runner path
# ---------------------------------------------------------------------------

_RUNNER_SCENARIO = """
import os
import sys

sys.path.insert(0, {src!r})

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (AttemptOutputStore, DurableStorage, InMemorySource, SQLiteDurableStore)
from caravel.durability import commits as commits_module
from caravel.durability import sqlite as sqlite_module
from caravel.loaders import CallableLoader

CRASH_AT = {crash_at!r}


def crash(name):
    if name == CRASH_AT:
        sys.stdout.flush()
        os._exit({exit_code})


sqlite_module._failpoint_hook = crash
commits_module._failpoint_hook = crash


def load(payload, *, context):
    return {{"loaded": payload}}


def publish(payload, *, context):
    return {{"published": payload}}


pipeline = Pipeline(
    name="documents",
    stages=[
        Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))]),
        Stage(name="publish", entries=[Step(fn=publish, output=JSONDataset(name="publish"))]),
    ],
)

store = SQLiteDurableStore({store_path!r})
result = caravel.run(
    pipeline,
    pipeline_id={pipeline_id!r},
    job_id={job!r},
    source=InMemorySource({{"p1": 1, "p2": 2}}),
    storage=DurableStorage(store, AttemptOutputStore({artifacts!r}), restart_safe=True),
)
store.close()
print("STATUS:" + result.status)
"""


def _run_runner_scenario(tmp_path: Path, crash_at: str) -> Any:
    import subprocess
    import sys as _sys
    import textwrap

    script = _RUNNER_SCENARIO.format(
        src=str(Path(__file__).resolve().parents[2] / "src"),
        crash_at=crash_at,
        exit_code=137,
        store_path=str(tmp_path / "durable.sqlite3"),
        artifacts=str(tmp_path / "artifacts"),
        pipeline_id=PIPELINE_ID,
        job=JOB,
    )
    return subprocess.run(
        [_sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.parametrize(
    "crash_at",
    [
        "after_batch_insert",
        "after_batch_progress",
        "after_publication",
        "after_unit_sealed",
        "after_dependencies_activated",
        "after_node_seals",
        "after_job_completed",
    ],
)
def test_a_hard_kill_through_the_runner_replays_only_uncommitted_work(
    tmp_path: Path, crash_at: str
) -> None:
    killed = _run_runner_scenario(tmp_path, crash_at)
    assert killed.returncode == 137, f"stdout={killed.stdout} stderr={killed.stderr}"
    assert "STATUS:" not in killed.stdout

    resumed = _run_runner_scenario(tmp_path, "never")
    assert resumed.returncode == 0, resumed.stderr
    assert "STATUS:completed" in resumed.stdout

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    output = AttemptOutputStore(tmp_path / "artifacts")
    try:
        publisher = BatchPublisher(store, output)
        job = store.load_job(JOB)
        assert job is not None and job.status == "completed"
        for node_id, dataset_name in (
            ("stage-001-entry-001", "load"),
            ("stage-002-entry-001", "publish"),
        ):
            for partition_id in ("p1", "p2"):
                key = WorkUnitKey(JOB, node_id, partition_id)
                unit = store.load_work_unit(key)
                assert unit is not None and unit.status == "sealed"
                references = publisher.committed_references(key)
                # One logical batch can never yield two committed manifest entries.
                assert len(references) == 1
                assert unit.batch_count == 1
                assert (
                    len(publisher.load_work_unit(key, dataset=JSONDataset(name=dataset_name))) == 1
                )
    finally:
        store.close()


def test_an_orphaned_executing_unit_is_reclaimed_with_a_higher_fence(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    """A unit abandoned mid-execution must not strand the job."""
    source = InMemorySource({"p1": {"n": 1}})
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))])],
    )
    plan = compile_durable_plan(pipeline)
    from caravel.durability.scheduler import register_plan_discovery

    job = store.initialize_job(
        JOB,
        pipeline_id=PIPELINE_ID,
        source_mode="snapshot",
        declaration_fingerprint="decl",
    )
    register_plan_discovery(store, job, plan, source.discover())

    key = WorkUnitKey(JOB, "stage-001-entry-001", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    abandoned = store.claim_work_unit(
        key, owner_id="dead-worker", expected_generation=unit.generation, lease_seconds=3600
    )

    # The abandoned unit is invisible to eligible work. Its lease is still live,
    # so only a sole worker may reclaim it; a competing worker must wait it out.
    assert store.eligible_work(JOB, limit=10) == ()
    assert store.reclaimable_work(JOB, limit=10) == ()
    reclaimable = store.reclaimable_work(JOB, limit=10, include_live=True)
    assert [item.key for item in reclaimable] == [key]

    taken = store.claim_work_unit(
        key,
        owner_id="live-worker",
        expected_generation=abandoned.generation,
        exclusive=True,
    )
    assert taken.owner_id == "live-worker"
    assert taken.fence_token > abandoned.fence_token
    assert taken.claim_epoch > abandoned.claim_epoch

    # The displaced worker can no longer commit.
    with pytest.raises(FenceViolationError):
        store.commit_batch(
            key,
            owner_id="dead-worker",
            fence_token=abandoned.fence_token,
            expected_generation=taken.generation,
            batch_id="batch-000000",
            output_ref="output://stale",
        )


def test_takeover_of_a_live_claim_is_refused_without_an_explicit_opt_in(
    store: SQLiteDurableStore, output: AttemptOutputStore
) -> None:
    source = InMemorySource({"p1": {"n": 1}})
    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))])],
    )
    from caravel.durability.scheduler import register_plan_discovery

    job = store.initialize_job(
        JOB,
        pipeline_id=PIPELINE_ID,
        source_mode="snapshot",
        declaration_fingerprint="decl",
    )
    register_plan_discovery(store, job, compile_durable_plan(pipeline), source.discover())

    key = WorkUnitKey(JOB, "stage-001-entry-001", "p1")
    unit = store.load_work_unit(key)
    assert unit is not None
    held = store.claim_work_unit(
        key, owner_id="worker-a", expected_generation=unit.generation, lease_seconds=3600
    )
    with pytest.raises(FenceViolationError, match="live owner"):
        store.claim_work_unit(key, owner_id="worker-b", expected_generation=held.generation)


@pytest.mark.parametrize("limit", [0, -1, 2.5])
def test_reclaimable_work_rejects_an_invalid_limit(store: SQLiteDurableStore, limit: Any) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        store.reclaimable_work(JOB, limit=limit)
