"""Public output identity is declared, never the private aggregate execution ID.

``inputs.all(...)`` chooses input availability and readiness. It does not choose
an output name, merge payloads, or create an application-visible execution
partition. These tests address output the way an application does -- published
paths, ``results.of()``, ``results.published()``, the completion manifest, and
the batch feed -- and assert the reserved aggregate sentinel appears in none of
them.

They exercise real artifacts rather than substituting a string in a path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    BytesDataset,
    DurableStorage,
    InMemorySource,
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
    TextDataset,
    run,
)
from caravel.durability import AGGREGATE_PARTITION_ID, read_job_manifest, validate_job_manifest

DOCUMENTS = {f"doc-{index}": {"id": f"doc-{index}", "words": index} for index in range(1, 4)}


# -- processors -------------------------------------------------------------


def load(inputs, *, context: Any) -> Any:
    [partition] = inputs.partitions()
    document: Any = partition.load()
    return {**document, "loaded": True}


def summarize(inputs, *, context: Any) -> Any:
    """Read every committed partition and return one summary."""
    del context
    ids = sorted(partition.partition_id for partition in inputs.partitions())
    return {"documents": len(ids), "ids": ids}


def describe(inputs, *, context: Any) -> Any:
    del context
    return f"documents={sum(1 for _ in inputs.partitions())}"


def pack(inputs, *, context: Any) -> Any:
    del context
    return f"documents={sum(1 for _ in inputs.partitions())}".encode("utf-8")


def shard(inputs, *, context: Any) -> Any:
    """An all-mode callback that returns separately keyed records."""
    del context
    return {
        f"en/{partition.partition_id}": {"id": partition.partition_id}
        for partition in inputs.partitions()
    }


def restate(inputs, *, context: Any) -> Any:
    """An each-mode step downstream of a barrier."""
    [partition] = inputs.partitions()
    del context
    payload: Any = partition.load()
    return {"restated": payload}


# -- helpers ----------------------------------------------------------------


def _published_files(root: Path) -> list[str]:
    return sorted(
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and ".caravel" not in path.parts
    )


def _run(pipeline: Pipeline, tmp_path: Path, *, job_id: str = "j1") -> DurableStorage:
    storage = DurableStorage.at(tmp_path / "output")
    assert run(pipeline, job_id=job_id, source=InMemorySource(DOCUMENTS), storage=storage).status
    return storage


# -- explicit and default dataset naming ------------------------------------


def test_hierarchical_source_identity_remains_addressable(tmp_path: Path) -> None:
    pipeline = Pipeline(name="documents", stages=[Stage(name="s", entries=[Step(load)])])
    with DurableStorage.at(tmp_path / "output") as storage:
        assert (
            run(
                pipeline,
                job_id="j1",
                source=InMemorySource({"en/doc": {"words": 1}}),
                storage=storage,
            ).status
            == "completed"
        )
        results = storage.results(pipeline, job_id="j1")
        assert results.of("load")["en/doc"]["loaded"] is True
        assert all(Path(path).is_file() for path in results.published("load")["en/doc"])


def test_changed_dataset_name_cannot_resume_completed_output(tmp_path: Path) -> None:
    from caravel.durability import InvalidTransitionError

    def build(name: str) -> Pipeline:
        return Pipeline(
            name="documents",
            stages=[
                Stage(
                    name="s",
                    entries=[Step(summarize, input_scope="all", output=JSONDataset(name=name))],
                )
            ],
        )

    with DurableStorage.at(tmp_path / "output") as storage:
        original = build("summary")
        run(original, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
        paths = storage.results(original, job_id="j1").published("summarize")["summary"]
        with pytest.raises(InvalidTransitionError):
            run(build("renamed"), job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
        assert all(Path(path).is_file() for path in paths)


def test_aggregate_output_uses_an_explicit_dataset_name(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    assert list(results.of("summarize")) == ["summary"]
    assert results.of("summarize")["summary"]["documents"] == 3
    assert list(results.published("summarize")) == ["summary"]
    assert results.published("summarize")["summary"][0].endswith(
        "002-finish/001-summarize/summary/summary.json"
    )
    assert (
        tmp_path / "output/documents/j1/002-finish/001-summarize/summary/summary.json"
    ).is_file()


def test_an_omitted_dataset_name_defaults_to_the_declared_step_name(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish", entries=[Step(summarize, input_scope="all", output=JSONDataset())]
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    assert list(results.of("summarize")) == ["summarize"]
    assert (
        tmp_path / "output/documents/j1/002-finish/001-summarize/summarize/summarize.json"
    ).is_file()


def test_a_declared_step_name_names_the_aggregate_output(tmp_path: Path) -> None:
    """Renaming the step renames the output; the node id never appears."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(name="finish", entries=[Step(summarize, input_scope="all", name="rollup")]),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    assert list(results.of("rollup")) == ["rollup"]
    assert (tmp_path / "output/documents/j1/002-finish/001-rollup/rollup/rollup.json").is_file()


@pytest.mark.parametrize(
    "processor,dataset,suffix,expected",
    [
        (summarize, JSONDataset(name="summary"), ".json", {"documents": 3}),
        (describe, TextDataset(name="summary"), ".txt", "documents=3"),
        (pack, BytesDataset(name="summary"), ".bin", b"documents=3"),
    ],
)
def test_json_text_and_bytes_aggregates_keep_declaration_naming(
    tmp_path: Path, processor: Any, dataset: Any, suffix: str, expected: Any
) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(processor, input_scope="all", output=dataset, name="rollup")],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    published = results.published("rollup")["summary"][0]
    assert published.endswith(f"002-finish/001-rollup/summary/summary{suffix}")
    committed = results.of("rollup")["summary"]
    if isinstance(expected, dict):
        assert committed["documents"] == expected["documents"]
    else:
        assert committed == expected


def test_a_partitioned_aggregate_keeps_its_returned_keys_separate(tmp_path: Path) -> None:
    """A callback-chosen partitioned payload is not merged, flattened, or fanned out."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[
                    Step(shard, input_scope="all", output=PartitionedJSONDataset(name="documents"))
                ],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    # One logical result, keyed by the dataset name; the returned mapping stays
    # inside it rather than becoming execution partitions.
    assert list(results.of("shard")) == ["documents"]
    assert sorted(results.of("shard")["documents"]) == ["en/doc-1", "en/doc-2", "en/doc-3"]

    step_dir = tmp_path / "output/documents/j1/002-finish/001-shard"
    assert sorted(str(p.relative_to(step_dir)) for p in step_dir.rglob("*.json")) == [
        "documents/en/doc-1.json",
        "documents/en/doc-2.json",
        "documents/en/doc-3.json",
    ]


# -- topologies -------------------------------------------------------------


def test_an_each_step_after_a_barrier_publishes_its_own_declared_name(tmp_path: Path) -> None:
    """Downstream each-mode work must not inherit the private aggregate sentinel."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
            Stage(name="after", entries=[Step(restate, output=JSONDataset(name="restated"))]),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    assert list(results.of("restate")) == ["restated"]
    assert (tmp_path / "output/documents/j1/003-after/001-restate/restated/restated.json").is_file()
    assert AGGREGATE_PARTITION_ID not in str(_published_files(tmp_path / "output"))


def test_repeated_barriers_each_publish_their_own_name(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="first",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="first"))],
            ),
            Stage(
                name="second",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="second"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    results = storage.results(pipeline, job_id="j1")

    assert list(results.of("summarize")) == ["first"]
    assert (tmp_path / "output/documents/j1/002-first/001-summarize/first/first.json").is_file()
    assert (tmp_path / "output/documents/j1/003-second/001-summarize/second/second.json").is_file()


def test_a_leading_barrier_publishes_its_declared_name(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="only",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            )
        ],
    )
    storage = _run(pipeline, tmp_path)

    assert list(storage.results(pipeline, job_id="j1").of("summarize")) == ["summary"]
    assert (tmp_path / "output/documents/j1/001-only/001-summarize/summary/summary.json").is_file()


def test_an_empty_aggregate_still_publishes_under_its_declared_name(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="only",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            )
        ],
    )
    storage = DurableStorage.at(tmp_path / "output")
    assert run(pipeline, job_id="j1", source=InMemorySource({}), storage=storage).status == (
        "completed"
    )

    results = storage.results(pipeline, job_id="j1")
    assert list(results.of("summarize")) == ["summary"]
    assert results.of("summarize")["summary"]["documents"] == 0
    assert (tmp_path / "output/documents/j1/001-only/001-summarize/summary/summary.json").is_file()


def test_a_branch_feeding_a_barrier_publishes_the_declared_name(tmp_path: Path) -> None:
    from caravel import Branch

    def wide_step(inputs, *, context: Any) -> Any:
        [partition] = inputs.partitions()
        del context
        return {**partition.load(), "route": "wide_step"}

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="route",
                entries=[
                    Branch(
                        name="pick",
                        by=lambda record: "wide_step",
                        routes={
                            "wide_step": [Step(wide_step, output=JSONDataset(name="wide_step"))]
                        },
                    )
                ],
            ),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)

    assert list(storage.results(pipeline, job_id="j1").of("summarize")) == ["summary"]
    assert (
        tmp_path / "output/documents/j1/003-finish/001-summarize/summary/summary.json"
    ).is_file()


# -- manifests and feeds ----------------------------------------------------


def test_the_completion_manifest_addresses_output_logically(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)

    manifest = read_job_manifest(tmp_path / "output", "documents", "j1")
    assert manifest is not None
    assert validate_job_manifest(manifest, storage.progress) == []

    text = json.dumps(manifest)
    assert AGGREGATE_PARTITION_ID not in text
    outputs = {entry["output"] for entry in manifest["outputs"]}
    assert "summary" in outputs
    assert all("partition_id" not in entry for entry in manifest["outputs"])
    aggregate = next(entry for entry in manifest["outputs"] if entry["output"] == "summary")
    assert aggregate["step"] == "summarize"
    assert aggregate["files"] == ["002-finish/001-summarize/summary/summary.json"]


def test_the_feed_addresses_aggregate_output_logically(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    feed = storage.results(pipeline, job_id="j1").feed("summarize")

    page = feed.read()
    assert [entry.logical_id for entry in page.entries] == ["summary"]
    payload = feed.load(page.entries[0], dataset=JSONDataset(name="summary"))
    assert payload["documents"] == 3


# -- lifecycle --------------------------------------------------------------


def test_recomputation_republishes_under_the_logical_name(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    first = storage.results(pipeline, job_id="j1").published("summarize")["summary"][0]

    storage.invalidate(pipeline, job_id="j1", step="summarize")
    assert run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage).status
    second = storage.results(pipeline, job_id="j1").published("summarize")["summary"][0]

    assert first != second, "a new generation gets its own location"
    assert "/summary/" in second or "/summary@g" in second
    assert AGGREGATE_PARTITION_ID not in second
    assert Path(second).is_file()


def test_retention_prunes_only_superseded_generations_of_the_logical_output(
    tmp_path: Path,
) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    assert run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage).status
    first = storage.results(pipeline, job_id="j1").published("summarize")["summary"][0]

    storage.invalidate(pipeline, job_id="j1", step="summarize")
    assert run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage).status
    current = storage.results(pipeline, job_id="j1").published("summarize")["summary"][0]

    assert Path(current).is_file()
    assert not Path(first).exists(), "the superseded generation is pruned by exact ownership"


def test_rebuilding_restores_the_logical_tree(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    target = Path(storage.results(pipeline, job_id="j1").published("summarize")["summary"][0])
    target.unlink()

    assert storage.rebuild_output(pipeline, job_id="j1") > 0
    assert target.is_file()
    assert json.loads(target.read_text())["documents"] == 3


def test_reopening_a_job_sees_the_same_logical_addresses(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    before = storage.results(pipeline, job_id="j1").published("summarize")
    storage.close()

    with DurableStorage.at(tmp_path / "output") as reopened:
        assert reopened.results(pipeline, job_id="j1").published("summarize") == before


def test_no_aggregate_sentinel_reaches_the_published_tree(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
            Stage(name="after", entries=[Step(restate, output=JSONDataset(name="restated"))]),
        ],
    )
    storage = _run(pipeline, tmp_path)
    files = _published_files(tmp_path / "output")

    assert files
    assert all(AGGREGATE_PARTITION_ID not in path for path in files)
    # The private identity is still what recovery state uses.
    assert AGGREGATE_PARTITION_ID in storage.progress.work_unit_partitions(
        "j1", "stage-002-entry-001"
    )


# -- validation and compatibility -------------------------------------------


@pytest.mark.parametrize("name", ["a/b", "..", "."])
def test_an_unusable_aggregate_output_name_is_rejected_before_any_mutation(
    tmp_path: Path, name: str
) -> None:
    """Compilation rejects it, so no source is read and no state is written."""
    from caravel.durability import UnsupportedDurableDeclarationError

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(
                name="only",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name=name))],
            )
        ],
    )
    storage = DurableStorage.at(tmp_path / "output")
    with pytest.raises(UnsupportedDurableDeclarationError, match="Output name"):
        run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)

    assert storage.progress.load_job("j1") is None
    assert not _published_files(tmp_path / "output")


def test_colliding_logical_names_are_rejected_rather_than_overwriting(tmp_path: Path) -> None:
    """Two units of one node may never address output by the same name."""
    from caravel.durability import LogicalIdentityError, LogicalOutputMap

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(load, output=JSONDataset(name="doc-1"))])],
    )
    storage = _run(pipeline, tmp_path)
    node = storage.results(pipeline, job_id="j1")._plan.nodes[0]

    # Source partitions address their own output, so these stay distinct.
    assert LogicalOutputMap.for_node(node, ["doc-1", "doc-2"]).to_logical == {
        "doc-1": "doc-1",
        "doc-2": "doc-2",
    }
    with pytest.raises(LogicalIdentityError, match="both as"):
        LogicalOutputMap.for_node(node, ["doc-1", "doc-1"])


def test_a_dotted_aggregate_name_owns_exactly_its_own_directory(tmp_path: Path) -> None:
    """Ownership is enumerated by exact name; `doc` must not claim `doc.other`."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="doc.other"))],
            ),
        ],
    )
    storage = DurableStorage.at(tmp_path / "output", keep_superseded=0)
    assert run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage).status

    step_dir = tmp_path / "output/documents/j1/002-finish/001-summarize"
    assert sorted(path.name for path in step_dir.iterdir()) == ["doc.other"]
    assert (step_dir / "doc.other/doc.other.json").is_file()

    storage.invalidate(pipeline, job_id="j1", step="summarize")
    assert run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage).status
    assert sorted(path.name for path in step_dir.iterdir()) == ["doc.other@g1"]


def test_a_store_written_under_an_older_layout_fails_closed(tmp_path: Path) -> None:
    """No dual layout: an incompatible store is rejected before any mutation."""
    import sqlite3

    from caravel.durability import UnsupportedStoreVersionError

    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = _run(pipeline, tmp_path)
    state = Path(str(storage.root)) / "state.sqlite3"
    storage.close()

    connection = sqlite3.connect(state)
    connection.execute("UPDATE schema_meta SET value = '1' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    published_before = _published_files(tmp_path / "output")
    with pytest.raises(UnsupportedStoreVersionError) as raised:
        DurableStorage.at(tmp_path / "output")
    # The message must name the supported way forward, not just the mismatch.
    assert "new job" in str(raised.value) and "rebuild" in str(raised.value)
    assert _published_files(tmp_path / "output") == published_before


def test_a_crash_before_marking_published_republishes_the_logical_output(
    tmp_path: Path,
) -> None:
    """Interrupting publication must not strand output under a different name."""
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(
                name="finish",
                entries=[Step(summarize, input_scope="all", output=JSONDataset(name="summary"))],
            ),
        ],
    )
    storage = DurableStorage.at(tmp_path / "output")
    original = storage.progress.mark_published
    node_id = "stage-002-entry-001"

    def crash_on_the_aggregate(key: Any, **kwargs: Any) -> bool:
        if key.node_id == node_id:
            raise RuntimeError("killed at the publication boundary")
        return bool(original(key, **kwargs))

    storage.progress.mark_published = crash_on_the_aggregate  # type: ignore[method-assign]
    run(pipeline, job_id="j1", source=InMemorySource(DOCUMENTS), storage=storage)
    storage.progress.mark_published = original  # type: ignore[method-assign]

    # The unit sealed but never recorded a publication, so it is still pending.
    assert storage.progress.unpublished_sealed_units("j1")

    plan = storage.results(pipeline, job_id="j1")._plan
    projection = storage.publisher("documents")
    assert projection.reconcile("j1", plan.nodes) == 1
    assert not storage.progress.unpublished_sealed_units("j1")
    results = storage.results(pipeline, job_id="j1")
    assert list(results.of("summarize")) == ["summary"]
    assert Path(results.published("summarize")["summary"][0]).is_file()
    assert all(AGGREGATE_PARTITION_ID not in path for path in _published_files(tmp_path / "output"))
