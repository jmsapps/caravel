"""Qualification of remote artifact storage against real object-storage backends.

Three claims are kept apart, because they prove different things:

1. *Locally qualified* — the remote artifact code path runs against a non-local
   fsspec store (``memory://``) in the ordinary suite. Proves the code path, not
   the protocol.
2. *Emulator-qualified* — this module against Azurite or moto: the real wire
   protocol, the real cloud SDK, real authentication, real listing and read-back
   over HTTP. Does **not** reproduce a cloud service's consistency model,
   latency, throttling, or IAM.
3. *Cloud-verified* — this module against the real service with real
   credentials. Only this closes the gap in (2).

Every reachable backend is discovered by ``conftest.py`` and each test runs
against all of them. The suite skips with a precise reason when none is
reachable, so a skip can never be mistaken for a pass.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    DurableStorage,
    InMemorySource,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)
from caravel.durability import BatchPublisher, WorkUnitKey

DOCUMENTS = {f"doc-{index}": {"id": f"doc-{index}", "n": index} for index in range(1, 4)}


@pytest.fixture()
def remote_root(backend: Any) -> str:
    """A unique prefix per test, so repeated qualification never collides."""
    return f"{backend.root}/{uuid.uuid4().hex}"


def _durable(tmp_path: Path, remote_root: str, backend: Any) -> DurableStorage:
    return DurableStorage.at(
        tmp_path / "output",
        artifacts_root=remote_root,
        storage_options=backend.storage_options or None,
    )


def load_partition(partition, *, context: Any) -> Any:
    document: Any = partition.load()
    return {**document, "loaded": True}


def load(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(load_partition)


def summarize_all(partitions, *, context: Any) -> Any:
    dataset = partitions
    return {"documents": sum(1 for _ in dataset.partitions())}


def summarize(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.all(summarize_all)


def _pipeline() -> Pipeline:
    return Pipeline(
        name="qualification",
        stages=[
            Stage(name="prepare", entries=[Step(load, output=JSONDataset(name="load"))]),
            Stage(
                name="summary",
                entries=[Step(summarize, output=JSONDataset(name="summary"))],
            ),
        ],
    )


def test_a_job_commits_and_reads_artifacts_on_real_object_storage(
    tmp_path: Path, remote_root: str, backend: Any
) -> None:
    """The supported envelope: local authoritative state, remote artifacts."""
    pipeline = _pipeline()
    storage = _durable(tmp_path, remote_root, backend)
    try:
        result = run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
        )
        assert result.status == "completed"
        assert result.restart_safe is True

        results = storage.results(pipeline, job_id="qualification-1")
        assert set(results.of("load")) == set(DOCUMENTS)
        assert list(results.of("summarize").values()) == [{"documents": len(DOCUMENTS)}]
    finally:
        storage.close()

    assert not (tmp_path / "artifacts").exists(), "artifacts were written locally"


def test_committed_artifacts_survive_reopening_the_store(
    tmp_path: Path, remote_root: str, backend: Any
) -> None:
    pipeline = _pipeline()
    first = _durable(tmp_path, remote_root, backend)
    try:
        run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=first,
        )
    finally:
        first.close()

    second = _durable(tmp_path, remote_root, backend)
    try:
        assert set(second.results(pipeline, job_id="qualification-1").of("load")) == set(DOCUMENTS)
    finally:
        second.close()


def test_resuming_reads_committed_remote_output_instead_of_recomputing(
    tmp_path: Path, remote_root: str, backend: Any
) -> None:
    calls: list[str] = []

    def counted_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        calls.append(document["id"])
        return {**document, "loaded": True}

    def counted(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(counted_partition)

    pipeline = Pipeline(
        name="qualification",
        stages=[Stage(name="prepare", entries=[Step(counted, output=JSONDataset(name="load"))])],
    )
    storage = _durable(tmp_path, remote_root, backend)
    try:
        run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
        )
        before = len(calls)
        again = run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
        )
        assert again.status == "completed"
        assert again.committed_batches == 0
        assert len(calls) == before, "committed remote output was recomputed"
    finally:
        storage.close()


def test_an_unreferenced_remote_attempt_is_invisible(
    tmp_path: Path, remote_root: str, backend: Any
) -> None:
    """Readers see only output reachable from a committed reference."""
    pipeline = _pipeline()
    storage = _durable(tmp_path, remote_root, backend)
    try:
        run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
        )
        key = WorkUnitKey("qualification-1", "stage-001-entry-001", "doc-1")
        publisher = BatchPublisher(storage.progress, storage.output)

        orphan = storage.output.write_attempt(
            key, "orphan", dataset=JSONDataset(name="load"), payload={"orphan": True}
        )
        assert storage.output.exists(orphan) is True
        assert publisher.committed_reference(key, "orphan") is None
        assert len(publisher.committed_references(key)) == 1
    finally:
        storage.close()


def test_recomputation_supersedes_remote_output_without_deleting_it(
    tmp_path: Path, remote_root: str, backend: Any
) -> None:
    pipeline = _pipeline()
    storage = _durable(tmp_path, remote_root, backend)
    try:
        run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
        )
        feed = storage.results(pipeline, job_id="qualification-1").feed("load")
        before = feed.read().entries

        storage.invalidate(pipeline, job_id="qualification-1", step="load")
        run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
        )
        after = feed.read().entries
        assert len(after) == len(before) * 2
        assert {entry.output_generation for entry in after} == {0, 1}
        # The superseded remote artifacts are still physically present.
        for entry in before:
            assert storage.output.exists(entry.reference) is True
    finally:
        storage.close()
