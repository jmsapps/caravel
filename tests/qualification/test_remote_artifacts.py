"""Credentialed qualification of remote artifact storage.

Two claims are kept apart deliberately:

- *Implemented and locally qualified* — the remote artifact code path runs
  against a non-local fsspec store in the ordinary suite.
- *Credentialed qualification passed* — this module, and only when it has
  actually run against real object storage.

It skips with a precise reason when no target is configured, so the skip can
never be mistaken for a pass. To run it:

    export CARAVEL_QUALIFICATION_ARTIFACTS_ROOT="az://<container>/<prefix>"
    # plus whatever that backend's SDK needs to authenticate, e.g.
    export AZURE_STORAGE_ACCOUNT_NAME=...     # adlfs
    python3 -m pytest -q tests/qualification

The root must be a container or prefix dedicated to qualification: the suite
writes committed artifacts under it.
"""

from __future__ import annotations

import os
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

ARTIFACTS_ROOT_VAR = "CARAVEL_QUALIFICATION_ARTIFACTS_ROOT"

_SKIP_REASON = (
    f"No credentialed target configured. Set {ARTIFACTS_ROOT_VAR} to an fsspec URL "
    "for a dedicated container or prefix (for example "
    "'az://caravel-qualification/run', 's3://bucket/prefix', 'gs://bucket/prefix') "
    "and authenticate that backend's SDK. Until this runs, remote artifact "
    "storage is IMPLEMENTED AND LOCALLY QUALIFIED, not cloud-verified."
)

DOCUMENTS = {f"doc-{index}": {"id": f"doc-{index}", "n": index} for index in range(1, 4)}


def _artifacts_root() -> str:
    root = os.getenv(ARTIFACTS_ROOT_VAR, "").strip()
    if not root:
        pytest.skip(_SKIP_REASON, allow_module_level=False)
    if "://" not in root:
        pytest.fail(
            f"{ARTIFACTS_ROOT_VAR}='{root}' is not an fsspec URL. A local path would "
            "qualify nothing that the ordinary suite does not already cover."
        )
    return root.rstrip("/")


def load(document: Any, *, context: Any) -> Any:
    return {**document, "loaded": True}


def summarize(dataset: Any, *, context: Any) -> Any:
    return {"documents": sum(1 for _ in dataset.partitions())}


def _pipeline() -> Pipeline:
    return Pipeline(
        name="qualification",
        stages=[
            Stage(name="prepare", entries=[Step(load, output=JSONDataset(name="load"))]),
            Stage(
                name="summary",
                entries=[Step(summarize, output=JSONDataset(name="summary"), input_scope="all")],
            ),
        ],
    )


@pytest.fixture()
def remote_root() -> str:
    """A unique prefix per run, so repeated qualification never collides."""
    return f"{_artifacts_root()}/{uuid.uuid4().hex}"


def test_a_job_commits_and_reads_artifacts_on_real_object_storage(
    tmp_path: Path, remote_root: str
) -> None:
    """The supported envelope: local authoritative state, remote artifacts."""
    pipeline = _pipeline()
    storage = DurableStorage.at(tmp_path, artifacts_root=remote_root)
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


def test_committed_artifacts_survive_reopening_the_store(tmp_path: Path, remote_root: str) -> None:
    pipeline = _pipeline()
    first = DurableStorage.at(tmp_path, artifacts_root=remote_root)
    try:
        run(
            pipeline,
            job_id="qualification-1",
            source=InMemorySource(DOCUMENTS),
            storage=first,
        )
    finally:
        first.close()

    second = DurableStorage.at(tmp_path, artifacts_root=remote_root)
    try:
        assert set(second.results(pipeline, job_id="qualification-1").of("load")) == set(DOCUMENTS)
    finally:
        second.close()


def test_resuming_reads_committed_remote_output_instead_of_recomputing(
    tmp_path: Path, remote_root: str
) -> None:
    calls: list[str] = []

    def counted(document: Any, *, context: Any) -> Any:
        calls.append(document["id"])
        return {**document, "loaded": True}

    pipeline = Pipeline(
        name="qualification",
        stages=[Stage(name="prepare", entries=[Step(counted, output=JSONDataset(name="load"))])],
    )
    storage = DurableStorage.at(tmp_path, artifacts_root=remote_root)
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


def test_an_unreferenced_remote_attempt_is_invisible(tmp_path: Path, remote_root: str) -> None:
    """Readers see only output reachable from a committed reference."""
    pipeline = _pipeline()
    storage = DurableStorage.at(tmp_path, artifacts_root=remote_root)
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
    tmp_path: Path, remote_root: str
) -> None:
    pipeline = _pipeline()
    storage = DurableStorage.at(tmp_path, artifacts_root=remote_root)
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
