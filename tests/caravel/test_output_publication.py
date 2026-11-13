"""Published output: readable, derived, and never a second authority."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    DurableStorage,
    ExecutionBudget,
    InMemorySource,
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
    TextDataset,
    run,
)
from caravel.durability import WorkUnitKey


from caravel.durability.publish import JOB_MANIFEST_NAME, readable_segment

JOB = "nightly-2026-11-13"
DOCUMENTS = {"doc-1": {"id": "doc-1", "n": 1}, "doc-2": {"id": "doc-2", "n": 2}}


def _simulate_crash_before_publishing(tmp_path: Path, *partition_ids: str) -> None:
    """Mark units sealed-but-unpublished, as a crash between the two would.

    ``mark_published`` deliberately refuses to record a generation that is not
    current, so this reaches past it to reproduce the interrupted state.
    """
    import sqlite3

    connection = sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3")
    try:
        for partition_id in partition_ids:
            connection.execute(
                "UPDATE work_units SET published_generation = -1 "
                "WHERE job_id = ? AND partition_id = ?",
                (JOB, partition_id),
            )
        connection.commit()
    finally:
        connection.close()


_VALUE = {"n": 1}


def load(document: Any, *, context: Any) -> Any:
    return {**document, "loaded": _VALUE["n"]}


def render(document: Any, *, context: Any) -> Any:
    return f"id={document['id']}"


@pytest.fixture(autouse=True)
def _reset() -> Any:
    _VALUE["n"] = 1
    yield
    _VALUE["n"] = 1


def _pipeline(*steps: Step) -> Pipeline:
    return Pipeline(
        name="documents", stages=[Stage(name="prepare", entries=list(steps or [Step(load)]))]
    )


def _run(tmp_path: Path, pipeline: Pipeline | None = None, **kwargs: Any) -> Any:
    storage = kwargs.pop("storage", None) or DurableStorage.at(tmp_path / "output")
    return run(
        pipeline or _pipeline(),
        job_id=kwargs.pop("job_id", JOB),
        source=kwargs.pop("source", InMemorySource(DOCUMENTS)),
        storage=storage,
        **kwargs,
    )


def _job_dir(tmp_path: Path) -> Path:
    return tmp_path / "output" / "documents" / JOB


# ---------------------------------------------------------------------------
# Internal state is separate from published output
# ---------------------------------------------------------------------------


def test_recovery_storage_is_internal_and_output_is_readable(tmp_path: Path) -> None:
    assert _run(tmp_path).status == "completed"

    internal = tmp_path / ".caravel"
    assert (internal / "state.sqlite3").exists()
    assert (internal / "attempts").exists(), "attempt artifacts stay internal"

    published = sorted(
        p.relative_to(_job_dir(tmp_path)) for p in _job_dir(tmp_path).rglob("*.json")
    )
    assert Path("001-prepare/001-load/doc-1/doc-1.json") in published
    # No uuid-named recovery paths leak into the published tree.
    assert not any("attempts" in str(p) for p in published)


def test_published_paths_use_declared_names(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(name="render", entries=[Step(render, output=TextDataset(name="render"))]),
        ],
    )
    _run(tmp_path, pipeline)
    assert (_job_dir(tmp_path) / "001-prepare" / "001-load" / "doc-1" / "doc-1.json").exists()
    assert (_job_dir(tmp_path) / "002-render" / "001-render" / "doc-2" / "doc-2.txt").exists()


def test_an_output_root_can_be_configured_independently(tmp_path: Path) -> None:
    elsewhere = tmp_path / "published"
    storage = DurableStorage.at(elsewhere, state=tmp_path / "state" / ".caravel")
    _run(tmp_path, storage=storage)
    assert (
        elsewhere / "documents" / JOB / "001-prepare" / "001-load" / "doc-1" / "doc-1.json"
    ).exists()
    assert (tmp_path / "state" / ".caravel" / "state.sqlite3").exists()
    assert not (elsewhere / ".caravel").exists(), "state must not nest under output"


def test_manifest_aware_access_is_the_authority_not_the_files(tmp_path: Path) -> None:
    """Published files are a projection; the store answers regardless of them."""
    import shutil

    storage = DurableStorage.at(tmp_path / "output")
    assert storage.publishes is True
    assert _run(tmp_path, storage=storage).status == "completed"

    shutil.rmtree(tmp_path / "output")
    assert set(storage.results(_pipeline(), job_id=JOB).of("load")) == set(DOCUMENTS)


# ---------------------------------------------------------------------------
# Only committed results appear
# ---------------------------------------------------------------------------


def test_a_failed_partition_publishes_nothing(tmp_path: Path) -> None:
    def explode(document: Any, *, context: Any) -> Any:
        if document["id"] == "doc-2":
            raise RuntimeError("boom")
        return {**document, "loaded": True}

    with pytest.raises(RuntimeError, match="boom"):
        _run(tmp_path, _pipeline(Step(explode)))

    step_dir = _job_dir(tmp_path) / "001-prepare" / "001-explode"
    assert (step_dir / "doc-1" / "doc-1.json").exists()
    assert not (step_dir / "doc-2" / "doc-2.json").exists(), "a failed attempt was published"
    assert not (_job_dir(tmp_path) / JOB_MANIFEST_NAME).exists()


def test_an_unreferenced_attempt_is_never_published(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage)

    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    orphan = storage.output.write_attempt(
        key, "orphan", dataset=JSONDataset(name="load"), payload={"orphan": True}
    )
    assert storage.output.exists(orphan)

    projection = storage.publisher("documents")
    node = storage.results(_pipeline(), job_id=JOB)._plan.nodes[0]
    projection.publish_unit(node, key)

    published = json.loads(
        (_job_dir(tmp_path) / "001-prepare" / "001-load" / "doc-1" / "doc-1.json").read_text()
    )
    assert "orphan" not in published


# ---------------------------------------------------------------------------
# Progressive visibility and the completion boundary
# ---------------------------------------------------------------------------


def test_a_partition_is_published_before_the_job_finishes(tmp_path: Path) -> None:
    import time

    def slow(document: Any, *, context: Any) -> Any:
        time.sleep(0.02)
        return {**document, "loaded": True}

    source = InMemorySource({f"doc-{i}": {"id": f"doc-{i}"} for i in range(8)})
    result = _run(
        tmp_path,
        _pipeline(Step(slow)),
        source=source,
        budget=ExecutionBudget(seconds=0.09, max_unit_seconds=0.03),
    )
    assert result.status == "yielded"

    step_dir = _job_dir(tmp_path) / "001-prepare" / "001-slow"
    published = list(step_dir.glob("*/*.json"))
    assert published, "nothing was visible before the job finished"
    assert len(published) < 8, "every partition finished; progressiveness unproven"
    # The whole-job boundary is absent while work remains.
    assert not (_job_dir(tmp_path) / JOB_MANIFEST_NAME).exists()

    assert _run(tmp_path, _pipeline(Step(slow)), source=source).status == "completed"
    assert (_job_dir(tmp_path) / JOB_MANIFEST_NAME).exists()


def test_the_completion_manifest_describes_published_output(tmp_path: Path) -> None:
    _run(tmp_path)
    manifest = json.loads((_job_dir(tmp_path) / JOB_MANIFEST_NAME).read_text())
    assert manifest["status"] == "completed"
    assert manifest["job_id"] == JOB
    assert {entry["partition_id"] for entry in manifest["outputs"]} == set(DOCUMENTS)
    assert all(entry["step"] == "load" for entry in manifest["outputs"])


# ---------------------------------------------------------------------------
# Interrupted publication recovers
# ---------------------------------------------------------------------------


def test_a_sealed_but_unpublished_unit_is_republished(tmp_path: Path) -> None:
    """Publication is asynchronous; the store records how far it got."""
    import shutil

    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage)

    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    target = _job_dir(tmp_path) / "001-prepare" / "001-load" / "doc-1" / "doc-1.json"
    # A crash between sealing and materializing leaves nothing published for
    # that partition at all.
    shutil.rmtree(target.parent)
    _simulate_crash_before_publishing(tmp_path, "doc-1")
    assert [u.key for u in storage.progress.unpublished_sealed_units(JOB)] == [key]

    projection = storage.publisher("documents")
    assert projection.reconcile(JOB, storage.results(_pipeline(), job_id=JOB)._plan.nodes) == 1
    assert target.exists()
    assert storage.progress.unpublished_sealed_units(JOB) == ()


# ---------------------------------------------------------------------------
# Recomputation replaces the current result
# ---------------------------------------------------------------------------


def test_recomputation_replaces_published_output(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    first = storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0]
    assert json.loads(Path(first).read_text())["loaded"] == 1

    _VALUE["n"] = 7
    storage.invalidate(pipeline, job_id=JOB, step="load")
    assert not (_job_dir(tmp_path) / JOB_MANIFEST_NAME).exists()

    _run(tmp_path, pipeline, storage=storage)
    current = storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0]
    assert json.loads(Path(current).read_text())["loaded"] == 7
    assert current != first, "a new generation reused the old published path"
    # Retained by default, and no longer what the current pointer names.
    assert Path(first).exists()


def test_pruning_removes_the_superseded_generation(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    first = storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0]

    _VALUE["n"] = 7
    storage.invalidate(pipeline, job_id=JOB, step="load")
    _run(tmp_path, pipeline, storage=storage)

    assert not Path(first).exists(), "pruning left the superseded generation"
    current = storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0]
    assert json.loads(Path(current).read_text())["loaded"] == 7


def test_recomputation_removes_files_a_smaller_result_no_longer_produces(
    tmp_path: Path,
) -> None:
    shards = {"n": 3}

    def shard(document: Any, *, context: Any) -> Any:
        return {f"part-{i}": {"v": i} for i in range(shards["n"])}

    pipeline = _pipeline(Step(shard, output=PartitionedJSONDataset(name="shard")))
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, pipeline, storage=storage)

    before = storage.results(pipeline, job_id=JOB).published("shard")["doc-1"]
    assert len(before) == 3

    shards["n"] = 1
    storage.invalidate(pipeline, job_id=JOB, step="shard")
    _run(tmp_path, pipeline, storage=storage)

    after = storage.results(pipeline, job_id=JOB).published("shard")["doc-1"]
    assert [Path(path).name for path in after] == ["part-0.json"]
    assert all(Path(path).exists() for path in after)
    # No superseded generation of any partition is left behind.
    step_dir = _job_dir(tmp_path) / "001-prepare" / "001-shard"
    assert sorted(p.name for p in step_dir.iterdir()) == [
        "doc-1",
        "doc-1@g1",
        "doc-2",
        "doc-2@g1",
    ]


# ---------------------------------------------------------------------------
# Naming safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["a/b", "..", ".", "with space", "unicode-é", "x" * 200, ""])
def test_readable_segments_are_safe(value: str) -> None:
    segment = readable_segment(value)
    assert "/" not in segment
    assert segment not in (".", "..")
    assert 0 < len(segment) <= 96


def test_distinct_identities_never_collapse_onto_one_path() -> None:
    assert readable_segment("a/b") != readable_segment("a-b")
    assert readable_segment("x" * 200 + "a") != readable_segment("x" * 200 + "b")


def test_unsafe_partition_ids_publish_safely(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage, source=InMemorySource({"a/b": {"id": "a/b"}}))
    files = list((_job_dir(tmp_path) / "001-prepare" / "001-load").glob("*/*.json"))
    assert len(files) == 1
    assert "/" not in files[0].name
    assert files[0].resolve().is_relative_to(tmp_path.resolve())


# ---------------------------------------------------------------------------
# One authority
# ---------------------------------------------------------------------------


def test_rebuild_restores_a_published_tree_deleted_behind_the_stores_back(
    tmp_path: Path,
) -> None:
    """The supported repair operation, exercised without touching SQLite by hand.

    ``reconcile`` deliberately will not do this: the store believes those units
    are published, so nothing is pending. ``rebuild_output`` is the operation
    that republishes regardless.
    """
    import shutil

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    shutil.rmtree(tmp_path / "output")
    # Deleting published files changes nothing about execution state.
    assert set(storage.results(pipeline, job_id=JOB).of("load")) == set(DOCUMENTS)

    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes
    assert projection.reconcile(JOB, nodes) == 0, "reconcile must not claim to repair this"

    assert storage.rebuild_output(pipeline, job_id=JOB) == len(DOCUMENTS)
    assert (_job_dir(tmp_path) / "001-prepare" / "001-load" / "doc-1" / "doc-1.json").exists()
    assert (_job_dir(tmp_path) / JOB_MANIFEST_NAME).exists()


def test_a_hierarchical_pipeline_id_nests_rather_than_flattening(
    tmp_path: Path,
) -> None:
    """``customer-42/documents`` is a hierarchy, not a name containing a slash."""
    storage = DurableStorage.at(tmp_path / "output")
    run(
        _pipeline(),
        pipeline_id="customer-42/documents",
        job_id=JOB,
        source=InMemorySource(DOCUMENTS),
        storage=storage,
    )
    nested = tmp_path / "output" / "customer-42" / "documents" / JOB
    assert (nested / "001-prepare" / "001-load" / "doc-1" / "doc-1.json").exists()
    assert (nested / JOB_MANIFEST_NAME).exists()


def test_a_traversing_pipeline_id_cannot_escape_the_output_root(
    tmp_path: Path,
) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    run(
        _pipeline(),
        pipeline_id="../../escape/documents",
        job_id=JOB,
        source=InMemorySource(DOCUMENTS),
        storage=storage,
    )
    output_root = (tmp_path / "output").resolve()
    written = [p for p in output_root.rglob("*.json")]
    assert written, "nothing was published"
    assert all(p.resolve().is_relative_to(output_root) for p in written)


# ---------------------------------------------------------------------------
# Review findings: each test fails against the behaviour it describes
# ---------------------------------------------------------------------------


def test_nested_dataset_paths_are_not_flattened(tmp_path: Path) -> None:
    """Finding 1: `en/record_1` and `fr/record_1` must not collide."""

    def shard(document: Any, *, context: Any) -> Any:
        return {
            "en/record_1": {"lang": "en"},
            "fr/record_1": {"lang": "fr"},
        }

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline(Step(shard, output=PartitionedJSONDataset(name="shard")))
    _run(tmp_path, pipeline, storage=storage, source=InMemorySource({"doc-1": {"id": "doc-1"}}))

    partition = _job_dir(tmp_path) / "001-prepare" / "001-shard" / "doc-1"
    assert json.loads((partition / "en" / "record_1.json").read_text())["lang"] == "en"
    assert json.loads((partition / "fr" / "record_1.json").read_text())["lang"] == "fr"


def test_a_stale_generation_publisher_cannot_overwrite_newer_output(
    tmp_path: Path,
) -> None:
    """Finding 2: a recomputation landing mid-publication must win.

    The dangerous window is between staging files and promoting them. This
    drives a publisher into that window, recomputes underneath it, and asserts
    the older bytes never reach the output tree.
    """
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    node = storage.results(pipeline, job_id=JOB)._plan.nodes[0]
    # Give the stale publisher work to do at generation 0.
    _simulate_crash_before_publishing(tmp_path, "doc-1")
    stale = storage.publisher("documents")

    recomputed: list[str] = []
    original_copy = stale._copy

    def recompute_midway(source: str, destination: str) -> None:
        original_copy(source, destination)
        if recomputed:
            return
        recomputed.append(destination)
        # A recomputation commits while this publisher holds staged bytes.
        _VALUE["n"] = 7
        storage.invalidate(pipeline, job_id=JOB, step="load")
        _run(tmp_path, pipeline, storage=storage)

    stale._copy = recompute_midway  # type: ignore[method-assign]
    stale.publish_unit(node, key)
    stale._copy = original_copy  # type: ignore[method-assign]

    assert recomputed, "the interleaving never happened; the test proves nothing"
    current = storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0]
    assert json.loads(Path(current).read_text())["loaded"] == 7, (
        "a stale generation's bytes reached the current published output"
    )
    unit = storage.progress.load_work_unit(key)
    assert unit is not None
    assert unit.published_generation == unit.output_generation


def test_mark_published_refuses_a_superseded_generation(tmp_path: Path) -> None:
    """Finding 2, at the store: the compare-and-set reports the loss."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    assert storage.progress.mark_published(key, generation=0) is True

    storage.invalidate(pipeline, job_id=JOB, step="load")
    assert storage.progress.mark_published(key, generation=0) is False


def test_a_crash_between_invalidation_and_cleanup_removes_the_marker_first(
    tmp_path: Path,
) -> None:
    """Finding 3: the marker must never survive over invalidated state."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    marker = _job_dir(tmp_path) / JOB_MANIFEST_NAME
    assert marker.exists()

    # Interrupt immediately after the database commits.
    original = storage.progress.invalidate_node

    def invalidate_then_crash(*args: Any, **kwargs: Any) -> Any:
        original(*args, **kwargs)
        raise RuntimeError("crash after commit, before cleanup")

    storage.progress.invalidate_node = invalidate_then_crash  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        storage.invalidate(pipeline, job_id=JOB, step="load")
    storage.progress.invalidate_node = original  # type: ignore[method-assign]

    assert not marker.exists(), "a stale completion marker survived invalidation"


def test_a_completion_marker_is_validated_against_authoritative_state(
    tmp_path: Path,
) -> None:
    """Finding 3: a reader with store access can detect a stale marker."""
    from caravel.durability import read_job_manifest, validate_job_manifest

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    manifest = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert manifest is not None
    assert validate_job_manifest(manifest, storage.progress) == []

    storage.progress.invalidate_node(JOB, "stage-001-entry-001")
    problems = validate_job_manifest(manifest, storage.progress)
    assert problems, "a superseded manifest validated clean"
    assert any("generation" in problem or "completed" in problem for problem in problems)


def test_publication_drains_more_units_than_one_page(tmp_path: Path) -> None:
    """Finding 4: a single capped fetch would leave the tail unpublished."""
    documents = {f"doc-{index:03d}": {"id": f"doc-{index:03d}"} for index in range(25)}
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage, source=InMemorySource(documents))

    _simulate_crash_before_publishing(tmp_path, *documents)
    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes

    # A page smaller than the backlog: draining must still finish all of it.
    assert projection.reconcile(JOB, nodes, page=5) == len(documents)
    assert storage.progress.unpublished_sealed_units(JOB) == ()
    published = list((_job_dir(tmp_path) / "001-prepare" / "001-load").glob("*/*.json"))
    assert len(published) == len(documents)


def test_the_completion_marker_is_withheld_while_publication_is_pending(
    tmp_path: Path,
) -> None:
    """Finding 4: completion must not be emitted over an incomplete projection."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    _simulate_crash_before_publishing(tmp_path, "doc-1")
    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes
    assert projection.write_job_manifest(JOB, nodes) is None

    projection.reconcile(JOB, nodes)
    assert projection.write_job_manifest(JOB, nodes) is not None


def test_every_manifest_reference_resolves_to_a_published_file(
    tmp_path: Path,
) -> None:
    """Finding 5: recorded paths must be the paths publication actually wrote."""

    def shard(document: Any, *, context: Any) -> Any:
        return {"a/one": {"v": 1}, "b/two": {"v": 2}}

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="prepare",
                entries=[
                    Step(load),
                    Step(shard, output=PartitionedJSONDataset(name="shard")),
                ],
            )
        ],
    )
    _run(tmp_path, pipeline, storage=storage)

    manifest = json.loads((_job_dir(tmp_path) / JOB_MANIFEST_NAME).read_text())
    assert manifest["outputs"]
    for entry in manifest["outputs"]:
        assert entry["files"], f"{entry['step']} recorded no files"
        for relative in entry["files"]:
            matches = list(_job_dir(tmp_path).rglob(relative))
            assert matches, f"manifest reference '{relative}' resolves to nothing"
            assert matches[0].is_file()


def test_reconcile_does_not_claim_to_repair_deleted_output(tmp_path: Path) -> None:
    """Finding 6: the rebuild guarantee belongs to rebuild, not reconcile."""
    import shutil

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    shutil.rmtree(tmp_path / "output")

    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes
    assert projection.reconcile(JOB, nodes) == 0
    assert not (tmp_path / "output").exists()

    assert storage.rebuild_output(pipeline, job_id=JOB) == len(DOCUMENTS)
    assert (_job_dir(tmp_path) / "001-prepare" / "001-load" / "doc-1" / "doc-1.json").exists()


def test_reserved_execution_storage_is_named_and_separate(tmp_path: Path) -> None:
    _run(tmp_path)
    assert (tmp_path / ".caravel" / "state.sqlite3").exists()
    assert (tmp_path / ".caravel" / "attempts").exists()
    assert not (tmp_path / "_internal").exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == [".caravel", "output"]


def test_state_in_the_superseded_location_is_not_silently_abandoned(
    tmp_path: Path,
) -> None:
    from caravel.durability import LegacyStateError

    legacy = tmp_path / "_internal"
    legacy.mkdir()
    (legacy / "state.sqlite3").write_bytes(b"")

    with pytest.raises(LegacyStateError, match="mv "):
        DurableStorage.at(tmp_path / "output")

    # Performing the documented move makes it resumable again.
    legacy.rename(tmp_path / ".caravel")
    assert DurableStorage.at(tmp_path / "output").publishes is True


# ---------------------------------------------------------------------------
# Interruption-point correctness (findings 1 and 2, second review)
# ---------------------------------------------------------------------------


def _current_payload(storage: DurableStorage, pipeline: Pipeline, step: str) -> Any:
    path = storage.results(pipeline, job_id=JOB).published(step)["doc-1"][0]
    return json.loads(Path(path).read_text())


def test_a_stale_publisher_that_dies_before_repair_leaves_current_output_intact(
    tmp_path: Path,
) -> None:
    """Finding 1: the four-step interleaving, with no repair afterwards.

    1. an old publisher passes its final generation check,
    2. invalidation and a newer publication complete,
    3. the old publisher resumes and promotes,
    4. it is killed before it can lose the compare-and-set and repair.

    The documented reader contract must still hold: the store names the current
    generation, and the file it names holds the new content.
    """
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    node = storage.results(pipeline, job_id=JOB)._plan.nodes[0]
    _simulate_crash_before_publishing(tmp_path, "doc-1")

    stale = storage.publisher("documents")
    interleaved: list[str] = []

    class _Killed(RuntimeError):
        pass

    original_mark = stale._store.mark_published

    def die_before_repair(*args: Any, **kwargs: Any) -> Any:
        # Step 4: terminate at the compare-and-set, before any repair loop.
        raise _Killed("publisher died before repairing")

    original_copy = stale._copy

    def recompute_after_check(source: str, destination: str) -> None:
        original_copy(source, destination)
        if interleaved:
            return
        interleaved.append(destination)
        # Steps 1-2: a newer generation is committed and published while this
        # publisher holds staged generation-0 bytes.
        _VALUE["n"] = 7
        storage.invalidate(pipeline, job_id=JOB, step="load")
        _run(tmp_path, pipeline, storage=storage)

    stale._copy = recompute_after_check  # type: ignore[method-assign]
    stale._store.mark_published = die_before_repair  # type: ignore[method-assign]
    with pytest.raises(_Killed):
        stale.publish_unit(node, key)  # Step 3: resumes and promotes.
    stale._store.mark_published = original_mark  # type: ignore[method-assign]
    stale._copy = original_copy  # type: ignore[method-assign]

    assert interleaved, "the interleaving never happened; the test proves nothing"

    # Reader contract: the store names the current generation, and that file
    # holds the new content. The stale generation may still exist on disk, but
    # it is not what the contract points at.
    unit = storage.progress.load_work_unit(key)
    assert unit is not None and unit.output_generation == 1
    assert _current_payload(storage, pipeline, "load")["loaded"] == 7


def test_a_concurrent_marker_write_during_invalidation_does_not_survive(
    tmp_path: Path,
) -> None:
    """Finding 2: a publisher recreating the marker mid-invalidation."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    marker = _job_dir(tmp_path) / JOB_MANIFEST_NAME
    assert marker.exists()

    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes
    original = storage.progress.invalidate_node

    def invalidate_with_concurrent_marker(*args: Any, **kwargs: Any) -> Any:
        # A competing publisher recreates the marker after removal but before
        # the invalidation commits.
        projection.write_job_manifest(JOB, nodes)
        return original(*args, **kwargs)

    storage.progress.invalidate_node = invalidate_with_concurrent_marker  # type: ignore[method-assign]
    storage.invalidate(pipeline, job_id=JOB, step="load")
    storage.progress.invalidate_node = original  # type: ignore[method-assign]

    assert not marker.exists(), "a concurrently recreated marker survived invalidation"


def test_a_marker_written_across_an_invalidation_withdraws_itself(
    tmp_path: Path,
) -> None:
    """Finding 2: the narrow window after the writer's last state read.

    The writer checks the job is complete, then writes the file. An
    invalidation landing in between would otherwise leave a marker advertising
    completion over superseded state, so the writer re-reads afterwards and
    withdraws its own marker.
    """
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes
    marker = _job_dir(tmp_path) / JOB_MANIFEST_NAME
    marker.unlink()

    original_load = storage.progress.load_job
    seen: list[str] = []

    def invalidate_after_the_last_check(job_id: str) -> Any:
        job = original_load(job_id)
        # The writer's pre-write check reads completed state; the invalidation
        # commits immediately afterwards, before the file is written.
        if job is not None and job.status == "completed" and not seen:
            seen.append(job_id)
            storage.progress.invalidate_node(JOB, "stage-001-entry-001")
        return job

    storage.progress.load_job = invalidate_after_the_last_check  # type: ignore[method-assign]
    result = projection.write_job_manifest(JOB, nodes)
    storage.progress.load_job = original_load  # type: ignore[method-assign]

    assert seen, "the interleaving never happened; the test proves nothing"
    assert result is None, "a doomed marker was reported as written"
    assert not marker.exists(), "a marker written across an invalidation survived"


def test_a_store_less_reader_gets_a_consistent_historical_snapshot(
    tmp_path: Path,
) -> None:
    """The documented contract for a reader with no store access.

    Published files are write-once per generation, so every path a manifest
    names still holds exactly that generation's bytes. A manifest may be stale,
    but it is never mixed or corrupt.
    """
    from caravel.durability import read_job_manifest, validate_job_manifest

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    snapshot = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert snapshot is not None
    job_dir = _job_dir(tmp_path)
    for entry in snapshot["outputs"]:
        for relative in entry["files"]:
            assert json.loads((job_dir / relative).read_text())["loaded"] == 1

    _VALUE["n"] = 7
    storage.invalidate(pipeline, job_id=JOB, step="load")
    _run(tmp_path, pipeline, storage=storage)

    # Generations are retained by default, so the earlier snapshot still
    # resolves completely and still describes generation 0 exactly.
    for entry in snapshot["outputs"]:
        for relative in entry["files"]:
            assert json.loads((job_dir / relative).read_text())["loaded"] == 1
    # A reader with store access is told it is superseded.
    assert validate_job_manifest(snapshot, storage.progress)
    # The current marker describes the new generation.
    current = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert current is not None
    assert validate_job_manifest(current, storage.progress) == []


# ---------------------------------------------------------------------------
# Storage contract: output is primary, state is beside it
# ---------------------------------------------------------------------------


def test_durable_is_the_default_and_state_sits_beside_output(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output")
    assert storage.restart_safe is True
    assert Path(str(storage.root)) == tmp_path / ".caravel"
    _run(tmp_path, storage=storage)
    assert (tmp_path / ".caravel" / "state.sqlite3").exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == [".caravel", "output"]


def test_state_names_the_recovery_directory_itself(tmp_path: Path) -> None:
    """No second ``.caravel`` is created beneath an explicit --state."""
    state = tmp_path / "persistent" / ".caravel"
    storage = DurableStorage.at(tmp_path / "exports" / "output", state=state)
    _run(tmp_path, storage=storage)
    assert (state / "state.sqlite3").exists()
    assert not (state / ".caravel").exists()
    assert sorted(p.name for p in state.iterdir()) == ["attempts", "state.sqlite3"]


def test_ephemeral_keeps_published_output_where_durable_puts_it(
    tmp_path: Path,
) -> None:
    """Switching execution mode must not move results."""
    output = tmp_path / "output"
    durable = DurableStorage.at(output)
    _run(tmp_path, storage=durable, job_id="durable-1")
    durable_paths = durable.results(_pipeline(), job_id="durable-1").published("load")

    volatile = DurableStorage.ephemeral(output)
    assert volatile.restart_safe is False
    assert Path(str(volatile.output_root)) == output
    _run(tmp_path, storage=volatile, job_id="volatile-1")

    assert all(
        Path(p).parent == Path(durable_paths["doc-1"][0]).parent.parent / "001-load"
        or Path(p).exists()
        for p in volatile.results(_pipeline(), job_id="volatile-1").published("load")["doc-1"]
    )


def test_ephemeral_cleanup_preserves_published_files(tmp_path: Path) -> None:
    """Closing throwaway storage must not take the results with it."""
    output = tmp_path / "output"
    storage = DurableStorage.ephemeral(output)
    _run(tmp_path, storage=storage, job_id="volatile-1")
    published = [
        Path(path)
        for paths in storage.results(_pipeline(), job_id="volatile-1").published("load").values()
        for path in paths
    ]
    assert published
    state_dir = Path(str(storage.root))

    storage.close()

    assert not state_dir.exists(), "throwaway recovery storage was not cleaned up"
    for path in published:
        assert path.exists(), "publication was deleted with the recovery storage"
        assert json.loads(path.read_text())["loaded"] == 1


def test_recovery_storage_inside_output_is_rejected(tmp_path: Path) -> None:
    """Rebuilding or cleaning output must not be able to destroy the store."""
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="inside the output directory"):
        DurableStorage.at(output, state=output / ".caravel")


def test_output_inside_recovery_storage_is_rejected(tmp_path: Path) -> None:
    state = tmp_path / ".caravel"
    with pytest.raises(ValueError, match="inside recovery storage"):
        DurableStorage.at(state / "output", state=state)


def test_remote_output_requires_an_explicit_local_state(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="recovery storage cannot sit beside it"):
        DurableStorage.at("memory://exports/output")

    storage = DurableStorage.at("memory://exports/output", state=tmp_path / ".caravel")
    assert storage.restart_safe is True
    assert (tmp_path / ".caravel" / "state.sqlite3").exists()


def test_remote_recovery_storage_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="local filesystem path"):
        DurableStorage.at(tmp_path / "output", state="memory://state")


def test_the_previous_meaning_of_state_is_reported_not_silently_changed(
    tmp_path: Path,
) -> None:
    """``--state DIR`` used to mean the *parent* of the recovery directory."""
    from caravel.durability import LegacyStateError

    old_root = tmp_path / "previous"
    (old_root / ".caravel").mkdir(parents=True)
    (old_root / ".caravel" / "state.sqlite3").write_bytes(b"")

    with pytest.raises(LegacyStateError, match="recovery directory itself"):
        DurableStorage.at(tmp_path / "output", state=old_root)

    # Pointing at the recovery directory itself finds the existing job.
    assert DurableStorage.at(tmp_path / "output", state=old_root / ".caravel")


# ---------------------------------------------------------------------------
# Publication semantics: marker status, within-generation writes, retention
# ---------------------------------------------------------------------------


def test_a_writer_dying_after_the_write_leaves_a_stale_marker(tmp_path: Path) -> None:
    """Marker presence is advisory; only validation makes it a guarantee.

    The exact sequence no file-only protocol can prevent:

    1. the writer passes its last state check,
    2. invalidation commits and completes both removals,
    3. the writer writes ``_job.json``,
    4. it dies before re-reading the job.
    """
    from caravel.durability import read_job_manifest, validate_job_manifest

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    projection = storage.publisher("documents")
    nodes = storage.results(pipeline, job_id=JOB)._plan.nodes
    marker = _job_dir(tmp_path) / JOB_MANIFEST_NAME
    marker.unlink()

    original_load = storage.progress.load_job
    checks: list[str] = []

    class _Died(RuntimeError):
        pass

    def pass_check_then_invalidate_then_die(job_id: str) -> Any:
        job = original_load(job_id)
        if job is not None and job.status == "completed" and not checks:
            checks.append("passed")  # Step 1.
            storage.invalidate(pipeline, job_id=JOB, step="load")  # Step 2.
            return job
        if checks:
            raise _Died("writer died before re-reading the job")  # Step 4.
        return job

    storage.progress.load_job = pass_check_then_invalidate_then_die  # type: ignore[method-assign]
    with pytest.raises(_Died):
        projection.write_job_manifest(JOB, nodes)  # Step 3 writes the file.
    storage.progress.load_job = original_load  # type: ignore[method-assign]

    assert checks, "the interleaving never happened; the test proves nothing"
    # The documented contract: the stale marker is present, and presence alone
    # proves nothing. Validation is what catches it.
    stale = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert stale is not None, "this test is about a marker that survives"
    problems = validate_job_manifest(stale, storage.progress)
    assert problems, "a stale marker validated clean"

    job = storage.progress.load_job(JOB)
    assert job is not None and job.status == "open"


def test_republishing_a_generation_never_truncates_its_files(tmp_path: Path) -> None:
    """Write-once holds within a generation, not only across generations."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    node = storage.results(pipeline, job_id=JOB)._plan.nodes[0]
    target = Path(storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0])
    assert target.exists()

    projection = storage.publisher("documents")
    removed: list[str] = []
    original_remove = projection._remove

    def record_removals(path: str) -> None:
        removed.append(str(path))
        original_remove(path)

    projection._remove = record_removals  # type: ignore[method-assign]
    _simulate_crash_before_publishing(tmp_path, "doc-1")
    projection.publish_unit(node, key)
    projection._remove = original_remove  # type: ignore[method-assign]

    assert target.exists()
    assert json.loads(target.read_text())["loaded"] == 1
    assert str(target) not in removed, "the published file was removed before rewriting"


def test_a_rebuild_swaps_rather_than_truncating(tmp_path: Path) -> None:
    """Repair must overwrite, but still never leave a partial file."""
    storage = DurableStorage.at(tmp_path / "output")

    def shard(document: Any, *, context: Any) -> Any:
        return {"a": {"v": 1}, "b": {"v": 2}}

    pipeline = _pipeline(Step(shard, output=PartitionedJSONDataset(name="shard")))
    _run(tmp_path, pipeline, storage=storage)

    partition_dir = Path(
        storage.results(pipeline, job_id=JOB).published("shard")["doc-1"][0]
    ).parent
    (partition_dir / "a.json").write_text("corrupted", encoding="utf-8")

    assert storage.rebuild_output(pipeline, job_id=JOB) == len(DOCUMENTS)
    assert json.loads((partition_dir / "a.json").read_text())["v"] == 1
    assert sorted(p.name for p in partition_dir.glob("*.json")) == ["a.json", "b.json"]
    # No swap leftovers.
    step_dir = partition_dir.parent
    assert not [p for p in step_dir.iterdir() if "__trash__" in p.name]
    assert not [p for p in step_dir.iterdir() if "__staging__" in p.name]


def test_retention_keeps_superseded_generations_when_asked(tmp_path: Path) -> None:
    """Deleting a superseded generation breaks a reader holding its manifest."""
    from caravel.durability import read_job_manifest

    storage = DurableStorage.at(tmp_path / "output", keep_superseded=1)
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    snapshot = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert snapshot is not None
    job_dir = _job_dir(tmp_path)

    _VALUE["n"] = 7
    storage.invalidate(pipeline, job_id=JOB, step="load")
    _run(tmp_path, pipeline, storage=storage)

    # The earlier snapshot still resolves completely under this policy.
    for entry in snapshot["outputs"]:
        for relative in entry["files"]:
            assert json.loads((job_dir / relative).read_text())["loaded"] == 1
    # And the current generation is what the store names.
    current = storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0]
    assert json.loads(Path(current).read_text())["loaded"] == 7


def test_pruning_is_explicit_and_the_default_retains_generations(
    tmp_path: Path,
) -> None:
    """A manifest names specific generations, so retaining them is the default."""
    retaining = DurableStorage.at(tmp_path / "retained" / "output")
    assert retaining.keep_superseded is None

    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    _VALUE["n"] = 7
    storage.invalidate(pipeline, job_id=JOB, step="load")
    _run(tmp_path, pipeline, storage=storage)

    step_dir = _job_dir(tmp_path) / "001-prepare" / "001-load"
    assert sorted(p.name for p in step_dir.iterdir()) == ["doc-1@g1", "doc-2@g1"]


def test_retention_bounds_how_many_generations_accumulate(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=1)
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    for generation in range(2, 5):
        _VALUE["n"] = generation
        storage.invalidate(pipeline, job_id=JOB, step="load")
        _run(tmp_path, pipeline, storage=storage)

    step_dir = _job_dir(tmp_path) / "001-prepare" / "001-load"
    for partition in ("doc-1", "doc-2"):
        kept = sorted(p.name for p in step_dir.glob(f"{partition}*"))
        assert len(kept) == 2, f"{partition} kept {kept}"


def test_retention_rejects_a_negative_policy(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=-1)
    with pytest.raises(ValueError, match="keep_superseded"):
        storage.publisher("documents")


# ---------------------------------------------------------------------------
# Second-review findings
# ---------------------------------------------------------------------------


def _shard_pipeline() -> Pipeline:
    def shard(document: Any, *, context: Any) -> Any:
        return {"a": {"v": 1}}

    return _pipeline(Step(shard, output=PartitionedJSONDataset(name="shard")))


def test_retiring_one_partition_leaves_a_similarly_named_one_alone(
    tmp_path: Path,
) -> None:
    """Finding 1: ownership must not be inferred by stripping extensions.

    ``doc`` and ``doc.other`` are different partitions. Treating the second as
    the first plus an extension deletes it while retiring the first.
    """
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    pipeline = _shard_pipeline()
    _run(
        tmp_path,
        pipeline,
        storage=storage,
        source=InMemorySource({"doc": {"x": 1}, "doc.other": {"x": 2}}),
    )
    step_dir = _job_dir(tmp_path) / "001-prepare" / "001-shard"
    assert sorted(p.name for p in step_dir.iterdir()) == ["doc", "doc.other"]

    storage.invalidate(pipeline, job_id=JOB, step="shard", partitions=["doc"])
    survivors = sorted(p.name for p in step_dir.iterdir())
    assert "doc.other" in survivors, "a different partition's output was deleted"


def test_a_delayed_sweep_cannot_delete_a_newer_generation(tmp_path: Path) -> None:
    """Finding 2: a paused publisher resuming must not prune what became current."""
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    stale = storage.publisher("documents")
    node = storage.results(pipeline, job_id=JOB)._plan.nodes[0]
    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")

    _VALUE["n"] = 7
    storage.invalidate(pipeline, job_id=JOB, step="load")
    _run(tmp_path, pipeline, storage=storage)
    current = Path(storage.results(pipeline, job_id=JOB).published("load")["doc-1"][0])
    assert current.exists()

    # The generation-0 publisher finally runs its sweep.
    stale._sweep_superseded(node, key)

    assert current.exists(), "a delayed sweep deleted the current generation"
    assert json.loads(current.read_text())["loaded"] == 7


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda m: m.update(outputs=[]), "missing from the manifest"),
        (lambda m: m.update(job_generation=-999), "job_generation"),
        (lambda m: m.update(pipeline_id="somewhere-else"), "pipeline_id"),
        (lambda m: m.update(schema_version=99), "schema version"),
    ],
)
def test_manifest_validation_rejects_each_disagreement(
    tmp_path: Path, mutate: Any, expected: str
) -> None:
    """Finding 3: validation must enforce what it advertises."""
    import copy

    from caravel.durability import read_job_manifest, validate_job_manifest

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)

    manifest = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert manifest is not None
    assert validate_job_manifest(manifest, storage.progress) == []

    tampered = copy.deepcopy(manifest)
    mutate(tampered)
    problems = validate_job_manifest(tampered, storage.progress)
    assert problems, "a tampered manifest validated clean"
    assert any(expected in problem for problem in problems), problems


def test_manifest_validation_rejects_an_entry_that_is_not_sealed(
    tmp_path: Path,
) -> None:
    import copy

    from caravel.durability import read_job_manifest, validate_job_manifest

    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    manifest = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert manifest is not None

    tampered = copy.deepcopy(manifest)
    tampered["outputs"].append({**tampered["outputs"][0], "partition_id": "never-existed"})
    problems = validate_job_manifest(tampered, storage.progress)
    assert any("not sealed" in problem for problem in problems), problems


def test_a_failed_repair_keeps_the_previous_publication(tmp_path: Path) -> None:
    """Finding 4: losing the old directory because the new one failed is worse."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _shard_pipeline()
    _run(tmp_path, pipeline, storage=storage)

    partition_dir = Path(
        storage.results(pipeline, job_id=JOB).published("shard")["doc-1"][0]
    ).parent
    before = json.loads((partition_dir / "a.json").read_text())

    projection = storage.publisher("documents")
    node = storage.results(pipeline, job_id=JOB)._plan.nodes[0]
    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    original_move = projection._move
    calls: list[int] = []

    def fail_the_second_rename(source: str, destination: str) -> None:
        calls.append(1)
        if len(calls) == 2:  # the staging -> base rename
            raise OSError("promotion failed")
        original_move(source, destination)

    projection._move = fail_the_second_rename  # type: ignore[method-assign]
    with pytest.raises(OSError, match="promotion failed"):
        projection.publish_unit(node, key, repair=True)
    projection._move = original_move  # type: ignore[method-assign]

    assert partition_dir.exists(), "the previous publication was discarded"
    assert json.loads((partition_dir / "a.json").read_text()) == before


def test_delayed_invalidation_cleanup_preserves_new_publication(tmp_path: Path) -> None:
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    pipeline = _pipeline()
    _run(tmp_path, pipeline, storage=storage)
    original_invalidate = storage.progress.invalidate_node
    newer_paths: list[str] = []

    def invalidate_then_recompute(*args: Any, **kwargs: Any) -> Any:
        invalidated = original_invalidate(*args, **kwargs)
        # Another invocation finishes before the original caller cleans up.
        assert _run(tmp_path, pipeline, storage=storage).status == "completed"
        newer_paths.extend(storage.results(pipeline, job_id=JOB).published("load")["doc-1"])
        return invalidated

    storage.progress.invalidate_node = invalidate_then_recompute  # type: ignore[method-assign]
    storage.invalidate(pipeline, job_id=JOB, step="load", partitions=["doc-1"])
    assert newer_paths
    assert all(Path(path).is_file() for path in newer_paths)


@pytest.mark.parametrize("files", [["nonexistent.json"], [], None, ["../escape.json"]])
def test_manifest_validation_rejects_changed_file_references(tmp_path: Path, files: Any) -> None:
    from caravel.durability import read_job_manifest, validate_job_manifest

    storage = DurableStorage.at(tmp_path / "output")
    _run(tmp_path, storage=storage)
    manifest = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert manifest is not None
    assert validate_job_manifest(manifest, storage.progress) == []
    manifest["outputs"][0]["files"] = files
    assert any(
        "files disagree" in problem for problem in validate_job_manifest(manifest, storage.progress)
    )


def test_publication_references_survive_store_reopen(tmp_path: Path) -> None:
    from caravel.durability import read_job_manifest, validate_job_manifest

    with DurableStorage.at(tmp_path / "output") as storage:
        _run(tmp_path, storage=storage)
    manifest = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert manifest is not None
    with DurableStorage.at(tmp_path / "output") as reopened:
        assert validate_job_manifest(manifest, reopened.progress) == []


def test_legacy_publication_references_are_backfilled_by_rebuild(tmp_path: Path) -> None:
    import sqlite3

    from caravel.durability import read_job_manifest, validate_job_manifest

    pipeline = _pipeline()
    with DurableStorage.at(tmp_path / "output") as storage:
        _run(tmp_path, pipeline, storage=storage)
    manifest = read_job_manifest(tmp_path / "output", "documents", JOB)
    assert manifest is not None
    # An existing store predating publication reference tracking has no table.
    with sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3") as connection:
        connection.execute("DROP TABLE publication_files")
    with DurableStorage.at(tmp_path / "output") as reopened:
        assert validate_job_manifest(manifest, reopened.progress)
        reopened.rebuild_output(pipeline, job_id=JOB)
        assert validate_job_manifest(manifest, reopened.progress) == []


def test_an_interrupted_swap_is_recovered_on_the_next_publish(
    tmp_path: Path,
) -> None:
    """Finding 4: termination between the two renames must be recoverable."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _shard_pipeline()
    _run(tmp_path, pipeline, storage=storage)

    projection = storage.publisher("documents")
    node = storage.results(pipeline, job_id=JOB)._plan.nodes[0]
    key = WorkUnitKey(JOB, "stage-001-entry-001", "doc-1")
    partition_dir = Path(
        storage.results(pipeline, job_id=JOB).published("shard")["doc-1"][0]
    ).parent

    # Reproduce the state a kill between the renames leaves: the previous
    # publication under a trash name, nothing at the destination.
    trash = partition_dir.parent / f"{partition_dir.name}.__trash__deadbeef"
    partition_dir.rename(trash)
    assert not partition_dir.exists()

    projection.publish_unit(node, key, repair=True)

    assert partition_dir.exists(), "the interrupted swap was not recovered"
    assert json.loads((partition_dir / "a.json").read_text())["v"] == 1
    assert not [p for p in partition_dir.parent.iterdir() if "__trash__" in p.name]
