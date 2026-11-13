"""Durable branch execution: routing, merge readiness, and crash recovery.

The contract is in
``.skills/tasks/C-8-durable-partition-execution/references/branch-durable-contract.md``.
Routing happens at the branch boundary, not at source discovery, because a
predicate may read values earlier steps produced.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    Branch,
    DurableStorage,
    InMemorySource,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)
from caravel.durability import (
    InvalidTransitionError,
    UnsupportedDurableDeclarationError,
    WorkUnitKey,
)
from caravel.durability.scheduler import compile_durable_plan

DOCUMENTS = {f"p{i}": {"n": i} for i in range(1, 5)}
BRANCH_NODE = "stage-001-entry-002"
MERGE_NODE = "stage-001-entry-002-merge"


def prepare_partition(partition, *, context: Any) -> Any:
    """Produce the value the predicate routes on, proving it is not source-only."""
    record: Any = partition.load()
    return {**record, "lang": "en" if record["n"] % 2 else "fr"}


def prepare(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(prepare_partition)


def english_partition(partition, *, context: Any) -> Any:
    record: Any = partition.load()
    return {**record, "route": "en"}


def english(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(english_partition)


def french_partition(partition, *, context: Any) -> Any:
    record: Any = partition.load()
    return {**record, "route": "fr"}


def french(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(french_partition)


def _pipeline(**branch_kwargs: Any) -> Pipeline:
    routes = branch_kwargs.pop("routes", {"en": [Step(english)], "fr": [Step(french)]})
    return Pipeline(
        name="documents",
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(prepare),
                    Branch(
                        name="bylang",
                        by=lambda record: record["lang"],
                        routes=routes,
                        **branch_kwargs,
                    ),
                ],
            )
        ],
    )


def _run(tmp_path: Path, pipeline: Pipeline | None = None, **kwargs: Any) -> Any:
    storage = kwargs.pop("storage", None) or DurableStorage.at(tmp_path / "output")
    return run(
        pipeline or _pipeline(),
        job_id=kwargs.pop("job_id", "j1"),
        source=kwargs.pop("source", InMemorySource(DOCUMENTS)),
        storage=storage,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Routing happens at the branch boundary
# ---------------------------------------------------------------------------


def test_a_branch_routes_on_values_produced_by_an_earlier_step(
    tmp_path: Path,
) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    assert _run(tmp_path, pipeline, storage=storage).status == "completed"

    results = storage.results(pipeline, job_id="j1")
    assert set(results.of("english")) == {"p1", "p3"}
    assert set(results.of("french")) == {"p2", "p4"}
    assert set(results.of("bylang_merge")) == set(DOCUMENTS)


def test_the_route_decision_is_committed_per_partition(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage)
    for partition_id, expected in (("p1", "en"), ("p2", "fr")):
        unit = storage.progress.load_work_unit(WorkUnitKey("j1", BRANCH_NODE, partition_id))
        assert unit is not None
        assert unit.route_key == expected
        assert unit.status == "sealed"


def test_the_predicate_is_consulted_once_per_partition_even_across_invocations(
    tmp_path: Path,
) -> None:
    """A committed route is authoritative; re-invoking never re-decides it."""
    calls: list[str] = []

    def counting_predicate(record: Any) -> str:
        calls.append(record["lang"])
        return record["lang"]

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(prepare),
                    Branch(
                        name="bylang",
                        by=counting_predicate,
                        routes={"en": [Step(english)], "fr": [Step(french)]},
                    ),
                ],
            )
        ],
    )
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, pipeline, storage=storage)
    assert len(calls) == len(DOCUMENTS)

    _run(tmp_path, pipeline, storage=storage)
    assert len(calls) == len(DOCUMENTS), "the predicate ran again after completion"


def test_a_flapping_predicate_cannot_reroute_a_committed_partition(
    tmp_path: Path,
) -> None:
    """Crash safety comes from the committed decision, not from a stable predicate."""
    flips = {"n": 0}

    def flapping(record: Any) -> str:
        flips["n"] += 1
        return "en" if flips["n"] % 2 else "fr"

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(prepare),
                    Branch(
                        name="bylang",
                        by=flapping,
                        routes={"en": [Step(english)], "fr": [Step(french)]},
                    ),
                ],
            )
        ],
    )
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, pipeline, storage=storage, source=InMemorySource({"p1": {"n": 1}}))

    unit = storage.progress.load_work_unit(WorkUnitKey("j1", BRANCH_NODE, "p1"))
    assert unit is not None
    committed = unit.route_key

    _run(tmp_path, pipeline, storage=storage, source=InMemorySource({"p1": {"n": 1}}))
    again = storage.progress.load_work_unit(WorkUnitKey("j1", BRANCH_NODE, "p1"))
    assert again is not None and again.route_key == committed


def test_a_route_absent_from_the_declaration_fails_closed(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    with pytest.raises(KeyError, match="no route for key"):
        _run(
            tmp_path,
            _pipeline(routes={"en": [Step(english)]}),
            storage=storage,
            source=InMemorySource({"p2": {"n": 2}}),
        )


# ---------------------------------------------------------------------------
# Only the taken route does work
# ---------------------------------------------------------------------------


def test_only_the_assigned_route_registers_work(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage, source=InMemorySource({"p1": {"n": 1}}))

    plan = compile_durable_plan(_pipeline())
    english_node = plan.node_by_id(f"{BRANCH_NODE}-route-001-step-001")
    french_node = plan.node_by_id(f"{BRANCH_NODE}-route-002-step-001")

    assert storage.progress.work_unit_partitions("j1", english_node.node_id) == ("p1",)
    assert storage.progress.work_unit_partitions("j1", french_node.node_id) == ()


def test_an_unselected_route_never_blocks_completion(tmp_path: Path) -> None:
    """Routes nobody takes register nothing, so they cannot hold the job open."""
    storage = DurableStorage.at(tmp_path / "output")
    result = _run(tmp_path, storage=storage, source=InMemorySource({"p1": {"n": 1}}))
    assert result.status == "completed"
    job = storage.progress.load_job("j1")
    assert job is not None and job.status == "completed"


# ---------------------------------------------------------------------------
# Merge readiness is per-partition
# ---------------------------------------------------------------------------


def test_a_merge_waits_for_its_own_route_not_for_other_partitions(
    tmp_path: Path,
) -> None:
    import time

    def slow_english_partition(partition, *, context: Any) -> Any:
        record: Any = partition.load()
        time.sleep(0.02)
        return {**record, "route": "en"}

    def slow_english(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(slow_english_partition)

    from caravel import ExecutionBudget

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline(routes={"en": [Step(slow_english)], "fr": [Step(french)]})
    partial = _run(
        tmp_path,
        pipeline,
        storage=storage,
        budget=ExecutionBudget(seconds=0.09, max_unit_seconds=0.03),
    )
    assert partial.status == "yielded"

    # Some partition merged while others had not finished their route.
    merged = storage.progress.work_unit_partitions("j1", MERGE_NODE)
    sealed = [
        p
        for p in merged
        if (u := storage.progress.load_work_unit(WorkUnitKey("j1", MERGE_NODE, p)))
        and u.status == "sealed"
    ]
    assert len(sealed) < len(DOCUMENTS), "every merge finished; nothing was proven"

    assert _run(tmp_path, pipeline, storage=storage).status == "completed"


def test_a_merge_is_blocked_until_its_route_step_seals(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    from caravel.durability.scheduler import register_plan_discovery

    plan = compile_durable_plan(pipeline)
    job = storage.progress.initialize_job(
        "j1", pipeline_id="documents", source_mode="snapshot", declaration_fingerprint="f"
    )
    register_plan_discovery(storage.progress, job, plan, InMemorySource(DOCUMENTS).discover())
    merge = storage.progress.load_work_unit(WorkUnitKey("j1", MERGE_NODE, "p1"))
    assert merge is not None
    assert merge.pending_deps == 1, "the merge must wait on the branch before routing"


def test_whole_dataset_work_after_a_merge_still_needs_an_all_input_step(
    tmp_path: Path,
) -> None:
    """Per-partition merge does not subsume a whole-dataset merge."""

    def summarize_all(partitions, *, context: Any) -> Any:
        dataset = partitions
        return {"routes": sorted({p.load()["route"] for p in dataset.partitions()})}

    def summarize(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.all(summarize_all)

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="s",
                entries=[
                    Step(prepare),
                    Branch(
                        name="bylang",
                        by=lambda record: record["lang"],
                        routes={"en": [Step(english)], "fr": [Step(french)]},
                    ),
                ],
            ),
            Stage(
                name="summary",
                entries=[Step(summarize, output=JSONDataset(name="summary"))],
            ),
        ],
    )
    assert _run(tmp_path, pipeline, storage=storage).status == "completed"
    summary = list(storage.results(pipeline, job_id="j1").of("summarize").values())
    assert summary == [{"routes": ["en", "fr"]}]


# ---------------------------------------------------------------------------
# Declaration prohibitions
# ---------------------------------------------------------------------------


def test_a_route_step_may_not_declare_an_all_input_helper() -> None:
    def aggregate(partitions: Any, *, context: Any) -> Any:
        del context
        return {"count": len(partitions)}

    def routed(inputs: Any, *, context: Any) -> Any:
        del context
        return inputs.all(aggregate)

    with pytest.raises(UnsupportedDurableDeclarationError, match="after convergence"):
        compile_durable_plan(_pipeline(routes={"en": [Step(routed, output=JSONDataset(name="e"))]}))


def test_a_branch_must_declare_at_least_one_route() -> None:
    # Rejected during plan compilation, before any durable state is touched.
    with pytest.raises(ValueError, match="at least one route"):
        compile_durable_plan(_pipeline(routes={}))


def test_a_route_must_declare_at_least_one_step() -> None:
    with pytest.raises(UnsupportedDurableDeclarationError, match="declares no steps"):
        compile_durable_plan(_pipeline(routes={"en": []}))


def test_a_committed_route_cannot_be_overwritten(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage, source=InMemorySource({"p1": {"n": 1}}))
    key = WorkUnitKey("j1", BRANCH_NODE, "p1")
    sealed = storage.progress.load_work_unit(key)
    assert sealed is not None and sealed.route_key == "en"

    # Even a legitimate new claimant cannot re-decide a committed route.
    with pytest.raises(InvalidTransitionError, match="sealed"):
        storage.progress.claim_work_unit(
            key, owner_id="worker-x", expected_generation=sealed.generation
        )


# ---------------------------------------------------------------------------
# Crash recovery across the routing transaction
# ---------------------------------------------------------------------------

_SCENARIO = """
import os, sys
sys.path.insert(0, {src!r})

from caravel import Branch, DurableStorage, InMemorySource, Pipeline, Stage, Step, run
from caravel.durability import commits as commits_module
from caravel.durability import sqlite as sqlite_module

CRASH_AT = {crash_at!r}


def crash(name):
    if name == CRASH_AT:
        sys.stdout.flush()
        os._exit(137)


sqlite_module._failpoint_hook = crash
commits_module._failpoint_hook = crash


def prepare_partition(partition, *, context):
    r = partition.load()
    return {{**r, "lang": "en" if r["n"] % 2 else "fr"}}


def prepare(inputs, *, context):
    return inputs.each(prepare_partition)


def english_partition(partition, *, context):
    r = partition.load()
    return {{**r, "route": "en"}}


def english(inputs, *, context):
    return inputs.each(english_partition)


def french_partition(partition, *, context):
    r = partition.load()
    return {{**r, "route": "fr"}}


def french(inputs, *, context):
    return inputs.each(french_partition)


pipeline = Pipeline(
    name="documents",
    stages=[Stage(name="s", entries=[
        Step(prepare),
        Branch(name="bylang", by=lambda r: r["lang"],
               routes={{"en": [Step(english)], "fr": [Step(french)]}}),
    ])],
)

storage = DurableStorage.at(
    os.path.join({state!r}, "output"), state=os.path.join({state!r}, ".caravel")
)
result = run(pipeline, job_id="j1",
             source=InMemorySource({{"p1": {{"n": 1}}, "p2": {{"n": 2}}}}),
             storage=storage)
print("STATUS:" + result.status)
"""


def _scenario(tmp_path: Path, crash_at: str) -> Any:
    script = _SCENARIO.format(
        src=str(Path(__file__).resolve().parents[2] / "src"),
        crash_at=crash_at,
        state=str(tmp_path),
    )
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.parametrize(
    "crash_at",
    ["after_batch_progress", "after_route_registered", "after_branch_sealed"],
)
def test_a_crash_around_routing_leaves_the_partition_unrouted_or_fully_routed(
    tmp_path: Path, crash_at: str
) -> None:
    killed = _scenario(tmp_path, crash_at)
    assert killed.returncode == 137, f"{killed.stdout}\n{killed.stderr}"

    storage = DurableStorage.at(tmp_path / "output")
    try:
        plan = compile_durable_plan(_pipeline())
        for partition_id in ("p1", "p2"):
            unit = storage.progress.load_work_unit(WorkUnitKey("j1", BRANCH_NODE, partition_id))
            if unit is None or unit.route_key is None:
                # Unrouted: no route work may exist for this partition.
                for node in plan.nodes:
                    if node.branch_node_id and node.kind != "merge":
                        assert partition_id not in storage.progress.work_unit_partitions(
                            "j1", node.node_id
                        )
            else:
                # Fully routed: the chosen route's units exist.
                chain = plan.node_by_id(BRANCH_NODE).route_chain(unit.route_key)
                for member in chain:
                    assert partition_id in storage.progress.work_unit_partitions(
                        "j1", member.node_id
                    )
    finally:
        storage.close()

    resumed = _scenario(tmp_path, "never")
    assert resumed.returncode == 0, resumed.stderr
    assert "STATUS:completed" in resumed.stdout
