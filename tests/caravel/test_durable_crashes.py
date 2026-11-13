"""Hard-kill crash recovery tests for durable publication.

In-process failpoints prove transaction rollback but cannot prove durability
across process death. Every scenario here therefore runs in a real child
process that is killed with ``os._exit`` at a named boundary. The parent then
reopens the real store and asserts the authoritative invariants.

The crash hook is installed by the child script, which is test code. No
production module ever consults an environment variable to decide whether to
crash.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path
import pytest

from caravel.datasets import JSONDataset
from caravel.durability import (
    AttemptOutputStore,
    BatchPublisher,
    SQLiteDurableStore,
    WorkUnitKey,
)

JOB = "job-1"
PIPELINE = "customer-42/documents"
OWNER = "worker-a"
CRASH_EXIT_CODE = 137

_SCENARIO = """
import os
import sys

sys.path.insert(0, {src!r})

from caravel.datasets import JSONDataset
from caravel.durability import (
    AttemptOutputStore,
    BatchPublisher,
    DiscoveredPartition,
    DurableStorage,
    DiscoveredWorkUnit,
    SQLiteDurableStore,
    WorkUnitKey,
)
from caravel.durability import commits as commits_module
from caravel.durability import sqlite as sqlite_module

CRASH_AT = {crash_at!r}
JOB = "{job}"
PIPELINE = "{pipeline}"
OWNER = "{owner}"


def crash(name):
    if name == CRASH_AT:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit({exit_code})


sqlite_module._failpoint_hook = crash
commits_module._failpoint_hook = crash

store = SQLiteDurableStore({store_path!r})
output = AttemptOutputStore({artifacts!r})
publisher = BatchPublisher(store, output)
dataset = JSONDataset(name="docs")


def partition(partition_id, *nodes):
    return DiscoveredPartition(
        partition_id=partition_id,
        input_ref="source://" + partition_id,
        work_units=tuple(
            DiscoveredWorkUnit(
                node_id=node_id,
                step_revision="rev-1",
                params_fingerprint="params-1",
                depends_on_nodes=(nodes[index - 1],) if index > 0 else (),
            )
            for index, node_id in enumerate(nodes)
        ),
    )


# This scenario is resumable: every step is skipped when its durable evidence
# already exists, so running it again after a hard kill continues the same job
# instead of restarting it.
job = store.initialize_job(
    JOB,
    pipeline_id=PIPELINE,
    source_mode="incremental",
    declaration_fingerprint="decl-v1",
    range_start="pos-0",
    range_end="pos-100",
)

if not job.discovery_sealed:
    job = store.register_discovery(
        JOB,
        expected_generation=job.generation,
        partitions=[partition("p1", "load", "enrich")],
        next_position="cursor-1",
        seal=True,
    )

for node_id in ("load", "enrich"):
    key = WorkUnitKey(JOB, node_id, "p1")
    unit = store.load_work_unit(key)
    if unit.status == "sealed":
        continue
    claimed = store.claim_work_unit(key, owner_id=OWNER, expected_generation=unit.generation)
    commit = publisher.publish(
        key,
        owner_id=OWNER,
        fence_token=claimed.fence_token,
        expected_generation=claimed.generation,
        batch_id="b1",
        dataset=dataset,
        payload={{"node": node_id}},
        input_exhausted=True,
    )
    state = commit.state
    if not state.input_exhausted:
        state = store.mark_input_exhausted(
            key,
            owner_id=OWNER,
            fence_token=state.fence_token,
            expected_generation=state.generation,
        )
    store.seal_work_unit(
        key,
        owner_id=OWNER,
        fence_token=state.fence_token,
        expected_generation=state.generation,
    )

job = store.load_job(JOB)
if job.status != "completed":
    store.complete_job(JOB, expected_generation=job.generation, checkpoint_position="pos-100")
store.close()
print("COMPLETED")
"""


def _run_scenario(tmp_path: Path, crash_at: str) -> subprocess.CompletedProcess[str]:
    script = _SCENARIO.format(
        src=str(Path(__file__).resolve().parents[2] / "src"),
        crash_at=crash_at,
        exit_code=CRASH_EXIT_CODE,
        store_path=str(tmp_path / "durable.sqlite3"),
        artifacts=str(tmp_path / "artifacts"),
        job=JOB,
        pipeline=PIPELINE,
        owner=OWNER,
    )
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _assert_no_contradiction(store: SQLiteDurableStore, output: AttemptOutputStore) -> None:
    """Assert publication, work state, ownership, and eligibility all agree."""
    publisher = BatchPublisher(store, output)
    dataset = JSONDataset(name="docs")
    job = store.load_job(JOB)
    if job is None:
        # The job's own insert was rolled back; nothing downstream can exist.
        assert store.load_work_unit(WorkUnitKey(JOB, "load", "p1")) is None
        assert store.node_seals(JOB) == ()
        return

    for node_id in ("load", "enrich"):
        key = WorkUnitKey(JOB, node_id, "p1")
        unit = store.load_work_unit(key)
        if unit is None:
            continue
        references = publisher.committed_references(key)

        # Progress never claims more batches than the manifest holds.
        assert unit.batch_count == len(references)

        # Every committed reference is physically loadable.
        for reference in references:
            assert output.exists(reference) is True
        assert len(publisher.load_work_unit(key, dataset=dataset)) == len(references)

        # A sealed unit holds no claim and reached end of input.
        if unit.status == "sealed":
            assert unit.owner_id is None
            assert unit.input_exhausted is True
            assert unit.sealed_at is not None
        else:
            assert unit.sealed_at is None

    load_unit = store.load_work_unit(WorkUnitKey(JOB, "load", "p1"))
    enrich_unit = store.load_work_unit(WorkUnitKey(JOB, "enrich", "p1"))
    if load_unit is not None and enrich_unit is not None:
        # A dependent is activated if and only if its upstream is sealed.
        expected_pending = 0 if load_unit.status == "sealed" else 1
        assert enrich_unit.pending_deps == expected_pending
        if enrich_unit.pending_deps > 0:
            assert all(work.key.node_id != "enrich" for work in store.eligible_work(JOB, limit=10))

    # A completed job implies every unit sealed and the checkpoint advanced.
    checkpoint = store.load_checkpoint(PIPELINE, "default")
    if job.status == "completed":
        assert load_unit is not None and load_unit.status == "sealed"
        assert enrich_unit is not None and enrich_unit.status == "sealed"
        assert {seal.node_id for seal in store.node_seals(JOB)} == {"load", "enrich"}
        assert checkpoint is not None and checkpoint.completed_position == "pos-100"
    else:
        assert store.node_seals(JOB) == ()
        assert checkpoint is None or checkpoint.completed_position is None


CRASH_POINTS = [
    "after_job_insert",
    "after_discovery_insert",
    "after_discovery_advance",
    "before_attempt_write",
    "after_attempt_write",
    "after_batch_insert",
    "after_batch_progress",
    "after_publication",
    "after_unit_sealed",
    "after_dependencies_activated",
    "after_node_seals",
    "after_job_completed",
    "after_checkpoint_advanced",
]


@pytest.mark.parametrize("crash_at", CRASH_POINTS)
def test_a_hard_kill_at_a_durable_boundary_leaves_no_contradiction(
    tmp_path: Path, crash_at: str
) -> None:
    result = _run_scenario(tmp_path, crash_at)
    assert result.returncode == CRASH_EXIT_CODE, (
        f"child did not die at '{crash_at}': rc={result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    assert "COMPLETED" not in result.stdout

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        _assert_no_contradiction(store, AttemptOutputStore(tmp_path / "artifacts"))
    finally:
        store.close()


def test_the_uninterrupted_scenario_completes_and_is_durable(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "never")
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    assert "COMPLETED" in result.stdout

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    output = AttemptOutputStore(tmp_path / "artifacts")
    try:
        _assert_no_contradiction(store, output)
        job = store.load_job(JOB)
        assert job is not None and job.status == "completed"
        publisher = BatchPublisher(store, output)
        dataset = JSONDataset(name="docs")
        assert publisher.load_batch(WorkUnitKey(JOB, "load", "p1"), "b1", dataset=dataset) == {
            "node": "load"
        }
        assert publisher.load_batch(WorkUnitKey(JOB, "enrich", "p1"), "b1", dataset=dataset) == {
            "node": "enrich"
        }
    finally:
        store.close()


@pytest.mark.parametrize("crash_at", CRASH_POINTS)
def test_a_hard_kill_around_publication_replays_only_uncommitted_work(
    tmp_path: Path, crash_at: str
) -> None:
    """Resume the same job after the kill and assert nothing is committed twice."""
    killed = _run_scenario(tmp_path, crash_at)
    assert killed.returncode == CRASH_EXIT_CODE

    resumed = _run_scenario(tmp_path, "never")
    assert resumed.returncode == 0, f"resume failed: {resumed.stderr}"
    assert "COMPLETED" in resumed.stdout

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    output = AttemptOutputStore(tmp_path / "artifacts")
    try:
        _assert_no_contradiction(store, output)
        job = store.load_job(JOB)
        assert job is not None and job.status == "completed"
        for node_id in ("load", "enrich"):
            key = WorkUnitKey(JOB, node_id, "p1")
            unit = store.load_work_unit(key)
            if unit is None:
                continue
            # One logical batch id can never produce two committed manifest entries.
            references = BatchPublisher(store, output).committed_references(key)
            assert len(references) <= 1
            assert unit.batch_count == len(references)
    finally:
        store.close()


def test_an_orphaned_attempt_never_becomes_visible(tmp_path: Path) -> None:
    killed = _run_scenario(tmp_path, "after_attempt_write")
    assert killed.returncode == CRASH_EXIT_CODE

    attempts = tmp_path / "artifacts" / "attempts"
    assert attempts.exists(), "the child wrote an attempt before dying"

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    output = AttemptOutputStore(tmp_path / "artifacts")
    try:
        publisher = BatchPublisher(store, output)
        key = WorkUnitKey(JOB, "load", "p1")
        assert publisher.committed_references(key) == ()
        unit = store.load_work_unit(key)
        assert unit is not None and unit.batch_count == 0

        # The committed run that follows ignores the orphan entirely.
        resumed = _run_scenario(tmp_path, "never")
        assert resumed.returncode == 0, resumed.stderr
        reopened = BatchPublisher(store, output)
        assert len(reopened.committed_references(key)) == 1
        assert reopened.load_batch(key, "b1", dataset=JSONDataset(name="docs")) == {"node": "load"}
    finally:
        store.close()


def test_a_hard_kill_during_an_artifact_write_leaves_nothing_committed(
    tmp_path: Path,
) -> None:
    killed = _run_scenario(tmp_path, "before_attempt_write")
    assert killed.returncode == CRASH_EXIT_CODE

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        publisher = BatchPublisher(store, AttemptOutputStore(tmp_path / "artifacts"))
        key = WorkUnitKey(JOB, "load", "p1")
        assert publisher.committed_references(key) == ()
        unit = store.load_work_unit(key)
        assert unit is not None
        assert unit.status == "executing"
        assert unit.batch_count == 0
        assert unit.input_exhausted is False
    finally:
        store.close()


def test_the_store_reopens_cleanly_after_every_crash_point(tmp_path: Path) -> None:
    """A killed process must never leave the database unopenable."""
    for index, crash_at in enumerate(CRASH_POINTS):
        scenario_dir = tmp_path / f"run{index}"
        scenario_dir.mkdir()
        result = _run_scenario(scenario_dir, crash_at)
        assert result.returncode == CRASH_EXIT_CODE
        store = SQLiteDurableStore(scenario_dir / "durable.sqlite3")
        try:
            assert store.capabilities.missing() == ()
            _assert_no_contradiction(store, AttemptOutputStore(scenario_dir / "artifacts"))
        finally:
            store.close()
