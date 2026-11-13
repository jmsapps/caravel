"""Concurrent workers: real processes, a real store, and equivalent output.

Determinism comes from barrier-synchronized processes for genuine contention
rather than from sleeps, and from an injected clock wherever lease expiry
matters.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from caravel import JSONDataset
from caravel.durability import (
    AttemptOutputStore,
    BatchPublisher,
    LeasePolicy,
    SQLiteDurableStore,
    WorkUnitKey,
)

PIPELINE_ID = "customer-42/documents"
JOB = "snapshot-2026-11-12-v7"
LOAD_NODE = "stage-001-entry-001"
PUBLISH_NODE = "stage-002-entry-001"
PARTITIONS = [f"doc-{index:02d}" for index in range(8)]

_WORKER = """
import os
import sys

sys.path.insert(0, {src!r})

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    AttemptOutputStore,
    DurableStorage,
    ExecutionPolicy,
    InMemorySource,
    LeasePolicy,
    SQLiteDurableStore,
)
from caravel.durability import commits as commits_module
from caravel.durability import sqlite as sqlite_module
from caravel.loaders import CallableLoader

WORKER_ID = {worker_id!r}
CRASH_AT = {crash_at!r}
WORKERS = {workers}


def crash(name):
    if name == CRASH_AT:
        sys.stdout.flush()
        os._exit(137)


sqlite_module._failpoint_hook = crash
commits_module._failpoint_hook = crash


def load_partition(partition, *, context):
    payload = partition.load()
    return {{"loaded": payload}}


def load(inputs, *, context):
    return inputs.each(load_partition)


def publish_partition(partition, *, context):
    payload = partition.load()
    return {{"published": payload}}


def publish(inputs, *, context):
    return inputs.each(publish_partition)


pipeline = Pipeline(
    name="documents",
    stages=[
        Stage(name="prepare", entries=[Step(fn=load, output=JSONDataset(name="load"))]),
        Stage(name="publish", entries=[Step(fn=publish, output=JSONDataset(name="publish"))]),
    ],
)

partitions = {{"doc-%02d" % index: index for index in range({count})}}

store = SQLiteDurableStore({store_path!r}, timeout=30.0)

# All workers wait on this barrier file so they contend for real.
if {barrier!r}:
    import time
    start = time.monotonic()
    while not os.path.exists({barrier!r}):
        if time.monotonic() - start > 60:
            raise RuntimeError("barrier never released")
        time.sleep(0.005)

try:
    result = caravel.run(
        pipeline,
        pipeline_id={pipeline_id!r},
        job_id={job!r},
        source=InMemorySource(partitions),
        storage=DurableStorage(store, AttemptOutputStore({artifacts!r}), restart_safe=True),
        worker_id=WORKER_ID,
        policy=(
            ExecutionPolicy(workers=WORKERS, leases=LeasePolicy(ttl_seconds=30, heartbeat_seconds=5))
            if WORKERS > 1
            else ExecutionPolicy()
        ),
    )
    print("STATUS:" + result.status)
    print("BATCHES:%d" % result.committed_batches)
finally:
    store.close()
"""


def _spawn(
    tmp_path: Path,
    worker_id: str,
    *,
    workers: int = 2,
    crash_at: str = "never",
    barrier: str = "",
    count: int = len(PARTITIONS),
) -> "subprocess.Popen[str]":
    script = _WORKER.format(
        src=str(Path(__file__).resolve().parents[2] / "src"),
        worker_id=worker_id,
        crash_at=crash_at,
        workers=workers,
        count=count,
        store_path=str(tmp_path / "durable.sqlite3"),
        artifacts=str(tmp_path / "artifacts"),
        pipeline_id=PIPELINE_ID,
        job=JOB,
        barrier=barrier,
    )
    return subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run_concurrently(
    tmp_path: Path, worker_ids: list[str], *, crash_first: str = "never"
) -> list[tuple[int, str, str]]:
    """Start every worker, release them together, and collect their results."""
    barrier = str(tmp_path / "go")
    processes = [
        _spawn(
            tmp_path,
            worker_id,
            barrier=barrier,
            crash_at=crash_first if index == 0 else "never",
        )
        for index, worker_id in enumerate(worker_ids)
    ]
    Path(barrier).write_text("go", encoding="utf-8")

    results: list[tuple[int, str, str]] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=180)
        results.append((process.returncode, stdout, stderr))
    return results


def _assert_consistent(tmp_path: Path) -> dict[str, Any]:
    """Assert no duplicated work and return the committed output."""
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    output = AttemptOutputStore(tmp_path / "artifacts")
    try:
        publisher = BatchPublisher(store, output)
        collected: dict[str, Any] = {}
        for node_id, dataset_name in ((LOAD_NODE, "load"), (PUBLISH_NODE, "publish")):
            for partition_id in store.work_unit_partitions(JOB, node_id):
                key = WorkUnitKey(JOB, node_id, partition_id)
                unit = store.load_work_unit(key)
                assert unit is not None
                references = publisher.committed_references(key)
                # No duplicated work: one logical batch, one manifest entry.
                assert len(references) <= 1
                assert unit.batch_count == len(references)
                if unit.status == "sealed":
                    assert unit.owner_id is None
                    payloads = publisher.load_work_unit(key, dataset=JSONDataset(name=dataset_name))
                    collected[f"{node_id}/{partition_id}"] = payloads
        return collected
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Equivalence with sequential execution (coverage 6 and 7)
# ---------------------------------------------------------------------------


def test_concurrent_workers_produce_the_same_output_as_one_worker(
    tmp_path: Path,
) -> None:
    sequential_dir = tmp_path / "sequential"
    sequential_dir.mkdir()
    process = _spawn(sequential_dir, "solo", workers=1)
    stdout, stderr = process.communicate(timeout=180)
    assert process.returncode == 0, stderr
    assert "STATUS:completed" in stdout
    sequential = _assert_consistent(sequential_dir)

    concurrent_dir = tmp_path / "concurrent"
    concurrent_dir.mkdir()
    results = _run_concurrently(concurrent_dir, ["worker-a", "worker-b", "worker-c"])
    for code, stdout, stderr in results:
        assert code == 0, f"stdout={stdout} stderr={stderr}"
    assert any("STATUS:completed" in stdout for _, stdout, _ in results)
    concurrent = _assert_consistent(concurrent_dir)

    assert concurrent == sequential


def test_every_partition_is_committed_exactly_once_under_contention(
    tmp_path: Path,
) -> None:
    results = _run_concurrently(tmp_path, ["worker-a", "worker-b", "worker-c", "worker-d"])
    for code, stdout, stderr in results:
        assert code == 0, f"stdout={stdout} stderr={stderr}"

    collected = _assert_consistent(tmp_path)
    assert len(collected) == len(PARTITIONS) * 2

    # Across all workers, the total committed batch count equals the work count.
    total = sum(
        int(line.split(":")[1])
        for _, stdout, _ in results
        for line in stdout.splitlines()
        if line.startswith("BATCHES:")
    )
    assert total == len(PARTITIONS) * 2, "work was duplicated or lost"


def test_partition_order_and_dependencies_hold_under_concurrency(
    tmp_path: Path,
) -> None:
    results = _run_concurrently(tmp_path, ["worker-a", "worker-b", "worker-c"])
    for code, stdout, stderr in results:
        assert code == 0, stderr

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        for partition_id in PARTITIONS:
            upstream = store.load_work_unit(WorkUnitKey(JOB, LOAD_NODE, partition_id))
            dependent = store.load_work_unit(WorkUnitKey(JOB, PUBLISH_NODE, partition_id))
            assert upstream is not None and dependent is not None
            # A dependent never seals unless its upstream sealed first.
            if dependent.status == "sealed":
                assert upstream.status == "sealed"
                assert upstream.sealed_at is not None
                assert dependent.sealed_at is not None
                assert upstream.sealed_at <= dependent.sealed_at
        job = store.load_job(JOB)
        assert job is not None and job.status == "completed"
    finally:
        store.close()


def test_exactly_one_completion_commits_even_when_several_observe_it(
    tmp_path: Path,
) -> None:
    """Workers racing completion: one commits, the losers observe the result."""
    results = _run_concurrently(tmp_path, ["worker-a", "worker-b", "worker-c"])
    for code, stdout, stderr in results:
        assert code == 0, stderr

    # Several workers may legitimately report "completed": reinvoking a
    # completed job returns completed. What must be unique is the transition.
    assert any("STATUS:completed" in stdout for _, stdout, _ in results)

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        job = store.load_job(JOB)
        assert job is not None and job.status == "completed"
        # One seal per node, written by the single committing transition.
        seals = store.node_seals(JOB)
        assert len(seals) == 2
        assert {seal.node_id for seal in seals} == {LOAD_NODE, PUBLISH_NODE}
    finally:
        store.close()


def test_a_completion_race_advances_the_lineage_checkpoint_once(
    tmp_path: Path,
) -> None:
    """The generation check serializes completion; no new mechanism is needed."""
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        job = store.initialize_job(
            "range-1",
            pipeline_id=PIPELINE_ID,
            source_mode="incremental",
            declaration_fingerprint="decl",
            range_start="pos-0",
            range_end="pos-9",
        )
        job = store.register_discovery("range-1", expected_generation=job.generation, seal=True)

        from caravel.durability import InvalidTransitionError

        store.complete_job(
            "range-1", expected_generation=job.generation, checkpoint_position="pos-9"
        )
        with pytest.raises(InvalidTransitionError):
            store.complete_job(
                "range-1", expected_generation=job.generation, checkpoint_position="pos-9"
            )
        checkpoint = store.load_checkpoint(PIPELINE_ID, "default")
        assert checkpoint is not None
        assert checkpoint.generation == 1
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Worker death with competitors present (coverage 9)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "crash_at",
    [
        "after_batch_progress",
        "after_unit_sealed",
        "after_dependencies_activated",
        "after_node_seals",
        "after_job_completed",
    ],
)
def test_a_worker_dying_with_competitors_leaves_no_contradiction(
    tmp_path: Path, crash_at: str
) -> None:
    results = _run_concurrently(tmp_path, ["worker-a", "worker-b"], crash_first=crash_at)
    assert results[0][0] in (0, 137), f"unexpected exit: {results[0]}"

    _assert_consistent(tmp_path)

    # A later invocation finishes whatever the dead worker left behind.
    process = _spawn(tmp_path, "worker-recovery", workers=1)
    stdout, stderr = process.communicate(timeout=180)
    assert process.returncode == 0, stderr

    collected = _assert_consistent(tmp_path)
    assert len(collected) == len(PARTITIONS) * 2

    store = SQLiteDurableStore(tmp_path / "durable.sqlite3")
    try:
        job = store.load_job(JOB)
        assert job is not None and job.status == "completed"
    finally:
        store.close()


def test_a_dead_workers_claim_is_reclaimed_after_its_lease_expires(
    tmp_path: Path,
) -> None:
    """Expiry-gated takeover, with no sleeping: the clock is injected."""
    from datetime import datetime, timedelta, timezone

    from caravel.durability import DiscoveredPartition, DiscoveredWorkUnit
    from caravel.durability.leases import acquire

    now = datetime(2026, 11, 12, 9, 0, 0, tzinfo=timezone.utc)
    store = SQLiteDurableStore(tmp_path / "durable.sqlite3", clock=lambda: now)
    try:
        job = store.initialize_job(
            JOB,
            pipeline_id=PIPELINE_ID,
            source_mode="snapshot",
            declaration_fingerprint="decl",
        )
        store.register_discovery(
            JOB,
            expected_generation=job.generation,
            partitions=[
                DiscoveredPartition(
                    partition_id="p1",
                    input_ref="source://p1",
                    work_units=(
                        DiscoveredWorkUnit(
                            node_id="load",
                            step_revision="r1",
                            params_fingerprint="f1",
                        ),
                    ),
                )
            ],
            seal=True,
        )
        key = WorkUnitKey(JOB, "load", "p1")
        unit = store.load_work_unit(key)
        assert unit is not None
        dead = acquire(
            store,
            key,
            owner_id="worker-dead",
            expected_generation=unit.generation,
            policy=LeasePolicy(ttl_seconds=30, heartbeat_seconds=5),
        )

        # Before expiry, nobody may take it.
        from caravel.durability import FenceViolationError

        with pytest.raises(FenceViolationError):
            store.claim_work_unit(
                key,
                owner_id="worker-live",
                expected_generation=dead.state.generation,
                lease_seconds=30,
            )

        now = now + timedelta(seconds=31)
        taken = store.claim_work_unit(
            key,
            owner_id="worker-live",
            expected_generation=dead.state.generation,
            lease_seconds=30,
        )
        assert taken.owner_id == "worker-live"
        assert taken.fence_token > dead.fence_token
    finally:
        store.close()
