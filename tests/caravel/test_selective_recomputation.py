"""Selective recomputation: dependency-closure invalidation and output generations.

Design: ``references/selective-recomputation.md``. A bare reset of one node is
deliberately *not* what this does — it would leave dependents sealed against
input that no longer exists, silently.
"""

from __future__ import annotations

import datetime as dt
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
    InvalidTransitionError,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)
from caravel.durability import SQLiteDurableStore, WorkUnitKey

DOCUMENTS = {"p1": {"n": 1}, "p2": {"n": 2}}
LOAD_NODE = "stage-001-entry-001"
ENRICH_NODE = "stage-001-entry-002"

_VERSION = {"n": 1}


def load(record: Any, *, context: Any) -> Any:
    return {**record, "loaded": _VERSION["n"]}


def enrich(record: Any, *, context: Any) -> Any:
    return {**record, "enriched": record["loaded"] * 10}


@pytest.fixture(autouse=True)
def _reset_version() -> Any:
    _VERSION["n"] = 1
    yield
    _VERSION["n"] = 1


def _pipeline() -> Pipeline:
    return Pipeline(
        name="documents",
        stages=[Stage(name="s", entries=[Step(load), Step(enrich)])],
    )


def _run(storage: DurableStorage, pipeline: Pipeline | None = None) -> Any:
    return run(
        pipeline or _pipeline(),
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=storage,
    )


# ---------------------------------------------------------------------------
# Invalidation cascades
# ---------------------------------------------------------------------------


def test_invalidating_a_node_also_invalidates_what_was_derived_from_it(
    tmp_path: Path,
) -> None:
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    assert _run(storage, pipeline).status == "completed"

    invalidated = storage.invalidate(pipeline, job_id="j1", step="load")
    nodes = {key.node_id for key in invalidated}
    assert nodes == {LOAD_NODE, ENRICH_NODE}, "the cascade did not reach dependents"
    assert len(invalidated) == len(DOCUMENTS) * 2


def test_downstream_output_is_never_left_complete_against_stale_input(
    tmp_path: Path,
) -> None:
    """The exact failure a bare reset(job_id, node_id) would have produced."""
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)
    storage.invalidate(pipeline, job_id="j1", step="load")

    for node_id in (LOAD_NODE, ENRICH_NODE):
        for partition_id in DOCUMENTS:
            unit = storage.progress.load_work_unit(WorkUnitKey("j1", node_id, partition_id))
            assert unit is not None
            assert unit.status == "eligible"
            assert unit.sealed_at is None
            assert unit.batch_count == 0
    job = storage.progress.load_job("j1")
    assert job is not None and job.status == "open"
    assert storage.progress.node_seals("j1") == ()


def test_recomputation_produces_the_new_result(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)
    assert storage.results(pipeline, job_id="j1").of("enrich")["p1"]["enriched"] == 10

    _VERSION["n"] = 7
    storage.invalidate(pipeline, job_id="j1", step="load")
    assert _run(storage, pipeline).status == "completed"

    results = storage.results(pipeline, job_id="j1")
    assert results.of("load")["p1"]["loaded"] == 7
    assert results.of("enrich")["p1"]["enriched"] == 70


def test_an_upstream_outside_the_closure_is_not_recomputed(tmp_path: Path) -> None:
    """Edges into the closure from outside stay satisfied."""
    calls: list[str] = []

    def counted_load(record: Any, *, context: Any) -> Any:
        calls.append("load")
        return {**record, "loaded": 1}

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="s", entries=[Step(counted_load), Step(enrich)])],
    )
    storage = DurableStorage.at(tmp_path)
    _run(storage, pipeline)
    before = len(calls)

    storage.invalidate(pipeline, job_id="j1", step="enrich")
    assert _run(storage, pipeline).status == "completed"
    assert len(calls) == before, "an upstream outside the closure was recomputed"

    load_unit = storage.progress.load_work_unit(WorkUnitKey("j1", LOAD_NODE, "p1"))
    assert load_unit is not None and load_unit.status == "sealed"


def test_a_single_partition_can_be_invalidated(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)

    invalidated = storage.invalidate(pipeline, job_id="j1", step="load", partitions=["p1"])
    assert {key.partition_id for key in invalidated} == {"p1"}

    untouched = storage.progress.load_work_unit(WorkUnitKey("j1", LOAD_NODE, "p2"))
    assert untouched is not None and untouched.status == "sealed"


# ---------------------------------------------------------------------------
# Output generations
# ---------------------------------------------------------------------------


def test_invalidation_supersedes_output_without_deleting_it(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)

    feed = storage.results(pipeline, job_id="j1").feed("load")
    before = feed.read().entries
    assert all(entry.output_generation == 0 for entry in before)

    _VERSION["n"] = 7
    storage.invalidate(pipeline, job_id="j1", step="load")
    _run(storage, pipeline)

    after = feed.read().entries
    # Nothing was deleted; the new generation is appended at higher positions.
    assert len(after) == len(before) * 2
    generations = {entry.output_generation for entry in after}
    assert generations == {0, 1}
    newest = [e for e in after if e.output_generation == 1]
    assert all(e.position > max(b.position for b in before) for e in newest)


def test_readers_return_only_the_current_generation(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)
    _VERSION["n"] = 7
    storage.invalidate(pipeline, job_id="j1", step="load")
    _run(storage, pipeline)

    # One current payload per partition, not a mix of generations.
    loaded = storage.results(pipeline, job_id="j1").of("load")
    assert set(loaded) == set(DOCUMENTS)
    assert all(record["loaded"] == 7 for record in loaded.values())

    unit = storage.progress.load_work_unit(WorkUnitKey("j1", LOAD_NODE, "p1"))
    assert unit is not None and unit.output_generation == 1 and unit.batch_count == 1


def test_the_same_batch_id_may_be_recommitted_in_a_new_generation(
    tmp_path: Path,
) -> None:
    """Idempotency holds within a generation, not across them."""
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)
    storage.invalidate(pipeline, job_id="j1", step="load")
    assert _run(storage, pipeline).status == "completed"

    key = WorkUnitKey("j1", LOAD_NODE, "p1")
    current = storage.progress.lookup_batch(key, "batch-000000")
    assert current is not None and current.output_generation == 1


# ---------------------------------------------------------------------------
# Safety rules
# ---------------------------------------------------------------------------


def test_a_live_claim_refuses_the_invalidation(tmp_path: Path) -> None:
    moment = dt.datetime(2026, 11, 12, 9, 0, 0, tzinfo=dt.timezone.utc)
    storage = DurableStorage(
        SQLiteDurableStore(tmp_path / "s.sqlite3", clock=lambda: moment),
        DurableStorage.at(tmp_path / "a").output,
        restart_safe=True,
        root=tmp_path,
    )
    pipeline = _pipeline()
    _run(storage, pipeline)

    key = WorkUnitKey("j1", LOAD_NODE, "p1")
    unit = storage.progress.load_work_unit(key)
    assert unit is not None
    storage.progress.invalidate_node("j1", LOAD_NODE, partitions=["p1"])
    reopened = storage.progress.load_work_unit(key)
    assert reopened is not None
    storage.progress.claim_work_unit(
        key,
        owner_id="worker-a",
        expected_generation=reopened.generation,
        lease_seconds=300,
    )

    with pytest.raises(InvalidTransitionError, match="claimed by a live worker"):
        storage.invalidate(pipeline, job_id="j1", step="load", partitions=["p1"])


def test_an_expired_claim_does_not_block_invalidation(tmp_path: Path) -> None:
    moment = dt.datetime(2026, 11, 12, 9, 0, 0, tzinfo=dt.timezone.utc)
    clock = {"now": moment}
    storage = DurableStorage(
        SQLiteDurableStore(tmp_path / "s.sqlite3", clock=lambda: clock["now"]),
        DurableStorage.at(tmp_path / "a").output,
        restart_safe=True,
        root=tmp_path,
    )
    pipeline = _pipeline()
    _run(storage, pipeline)

    key = WorkUnitKey("j1", LOAD_NODE, "p1")
    storage.progress.invalidate_node("j1", LOAD_NODE, partitions=["p1"])
    reopened = storage.progress.load_work_unit(key)
    assert reopened is not None
    storage.progress.claim_work_unit(
        key,
        owner_id="worker-a",
        expected_generation=reopened.generation,
        lease_seconds=30,
    )
    clock["now"] = moment + dt.timedelta(seconds=31)
    assert storage.invalidate(pipeline, job_id="j1", step="load", partitions=["p1"])


def test_invalidating_an_unknown_step_fails_closed(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path)
    pipeline = _pipeline()
    _run(storage, pipeline)
    with pytest.raises(KeyError, match="No step named"):
        storage.invalidate(pipeline, job_id="j1", step="nonexistent")
    with pytest.raises(InvalidTransitionError, match="No work unit"):
        storage.progress.invalidate_node("j1", LOAD_NODE, partitions=["nope"])


def test_an_incremental_lineage_checkpoint_is_not_rolled_back(tmp_path: Path) -> None:
    import json

    from caravel import ChangeEvent, IncrementalInput, ListChangeAdapter

    events = [
        ChangeEvent(
            position=f"pos-{i}",
            entity_key=f"d{i}",
            operation="insert",
            revision=f"rev-{i}",
            payload_ref=json.dumps({"n": i}),
        )
        for i in range(2)
    ]
    storage = DurableStorage.at(tmp_path)
    pipeline = Pipeline(name="changes", stages=[Stage(name="s", entries=[Step(load)])])
    run(
        pipeline,
        job_id="r1",
        source=IncrementalInput(ListChangeAdapter(events)),
        storage=storage,
    )
    before = storage.progress.load_checkpoint("changes", "default")
    assert before is not None and before.completed_position == "pos-1"

    storage.progress.invalidate_node("r1", LOAD_NODE)
    after = storage.progress.load_checkpoint("changes", "default")
    assert after == before, "the range was completed; recomputation is a new generation"


def test_invalidating_a_branch_clears_its_committed_route(tmp_path: Path) -> None:
    def prepare(record: Any, *, context: Any) -> Any:
        return {**record, "lang": "en" if record["n"] % 2 else "fr"}

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
                        routes={
                            "en": [Step(load, output=JSONDataset(name="en"))],
                            "fr": [Step(load, output=JSONDataset(name="fr"))],
                        },
                    ),
                ],
            )
        ],
    )
    storage = DurableStorage.at(tmp_path)
    assert _run(storage, pipeline).status == "completed"

    branch_node = "stage-001-entry-002"
    before = storage.progress.load_work_unit(WorkUnitKey("j1", branch_node, "p1"))
    assert before is not None and before.route_key == "en"

    storage.invalidate(pipeline, job_id="j1", step="bylang")
    after = storage.progress.load_work_unit(WorkUnitKey("j1", branch_node, "p1"))
    assert after is not None
    assert after.route_key is None, "a re-decided branch must not keep its old route"

    assert _run(storage, pipeline).status == "completed"
    redecided = storage.progress.load_work_unit(WorkUnitKey("j1", branch_node, "p1"))
    assert redecided is not None and redecided.route_key == "en"


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------

_SCENARIO = """
import os, sys
sys.path.insert(0, {src!r})

from caravel import DurableStorage, InMemorySource, Pipeline, Stage, Step, run
from caravel.durability import sqlite as sqlite_module

CRASH_AT = {crash_at!r}


def crash(name):
    if name == CRASH_AT:
        sys.stdout.flush()
        os._exit(137)


def load(r, *, context):
    return {{**r, "loaded": 1}}


def enrich(r, *, context):
    return {{**r, "enriched": r["loaded"] * 10}}


pipeline = Pipeline(name="documents", stages=[Stage(name="s", entries=[Step(load), Step(enrich)])])
storage = DurableStorage.at({state!r})
run(pipeline, job_id="j1", source=InMemorySource({{"p1": {{"n": 1}}}}), storage=storage)

sqlite_module._failpoint_hook = crash
storage.invalidate(pipeline, job_id="j1", step="load")
print("INVALIDATED")
"""


@pytest.mark.parametrize("crash_at", ["after_invalidation", "after_job_reopened"])
def test_a_crash_during_invalidation_is_all_or_nothing(tmp_path: Path, crash_at: str) -> None:
    script = _SCENARIO.format(
        src=str(Path(__file__).resolve().parents[2] / "src"),
        crash_at=crash_at,
        state=str(tmp_path),
    )
    killed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert killed.returncode == 137, f"{killed.stdout}\n{killed.stderr}"

    storage = DurableStorage.at(tmp_path)
    try:
        load_unit = storage.progress.load_work_unit(WorkUnitKey("j1", LOAD_NODE, "p1"))
        enrich_unit = storage.progress.load_work_unit(WorkUnitKey("j1", ENRICH_NODE, "p1"))
        assert load_unit is not None and enrich_unit is not None
        job = storage.progress.load_job("j1")
        assert job is not None

        if load_unit.output_generation == 0:
            # Rolled back entirely: the job is still the completed original.
            assert enrich_unit.output_generation == 0
            assert load_unit.status == "sealed"
            assert job.status == "completed"
        else:
            # Committed entirely: the closure moved together.
            assert enrich_unit.output_generation == 1
            assert load_unit.status == "eligible"
            assert enrich_unit.status == "eligible"
            assert job.status == "open"
    finally:
        storage.close()
