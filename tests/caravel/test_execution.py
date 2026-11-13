"""Behaviour of the one execution model that survives consolidation.

The legacy single-pass runner's selective execution, ``clean_dirs``,
``stage_root``, ``context_factory``, and ``keep_source_tag`` are gone with it.
What remains is covered here; the public contract is in
``test_public_api_acceptance.py``.
"""

from __future__ import annotations

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
    TextDataset,
    run,
    step,
)

DOCUMENTS = {"doc-1": {"id": "doc-1", "n": 1}, "doc-2": {"id": "doc-2", "n": 2}}


def _run(pipeline: Pipeline, tmp_path: Path, **kwargs: Any) -> Any:
    storage = kwargs.pop("storage", None) or DurableStorage.at(tmp_path)
    return run(
        pipeline,
        job_id=kwargs.pop("job_id", "j1"),
        source=kwargs.pop("source", InMemorySource(DOCUMENTS)),
        storage=storage,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Step context
# ---------------------------------------------------------------------------


def test_params_reach_the_step_context(tmp_path: Path) -> None:
    seen: list[str] = []

    def capture(document: Any, *, context: Any) -> Any:
        seen.append(context.params["env"])
        return document

    _run(
        Pipeline(name="p", stages=[Stage(name="s", entries=[Step(capture)])]),
        tmp_path,
        params={"env": "staging"},
    )
    assert seen == ["staging"] * len(DOCUMENTS)


def test_the_context_identifies_the_step_and_its_stage(tmp_path: Path) -> None:
    seen: list[tuple[str, str, int]] = []

    def capture(document: Any, *, context: Any) -> Any:
        seen.append((context.stage_name, context.step_name, context.stage_index))
        return document

    _run(
        Pipeline(name="p", stages=[Stage(name="prepare", entries=[Step(capture)])]),
        tmp_path,
    )
    assert {item[:2] for item in seen} == {("prepare", "capture")}
    assert {item[2] for item in seen} == {1}


# ---------------------------------------------------------------------------
# Ordering and failure propagation
# ---------------------------------------------------------------------------


def test_steps_run_in_declaration_order_within_a_partition(tmp_path: Path) -> None:
    order: list[str] = []

    def first(document: Any, *, context: Any) -> Any:
        order.append("first")
        return document

    def second(document: Any, *, context: Any) -> Any:
        order.append("second")
        return document

    _run(
        Pipeline(name="p", stages=[Stage(name="s", entries=[Step(first), Step(second)])]),
        tmp_path,
        source=InMemorySource({"only": {"id": "only"}}),
    )
    assert order == ["first", "second"]


def test_a_step_failure_propagates_with_its_original_exception(tmp_path: Path) -> None:
    class VendorError(RuntimeError):
        pass

    def explode(document: Any, *, context: Any) -> Any:
        raise VendorError("vendor is down")

    with pytest.raises(VendorError, match="vendor is down"):
        _run(
            Pipeline(name="p", stages=[Stage(name="s", entries=[Step(explode)])]),
            tmp_path,
        )


def test_a_downstream_step_receives_the_upstream_payload(tmp_path: Path) -> None:
    def produce(document: Any, *, context: Any) -> Any:
        return {"produced": document["id"]}

    def consume(payload: Any, *, context: Any) -> Any:
        assert set(payload) == {"produced"}
        return {"consumed": payload["produced"]}

    storage = DurableStorage.at(tmp_path)
    pipeline = Pipeline(
        name="p",
        stages=[Stage(name="a", entries=[Step(produce)]), Stage(name="b", entries=[Step(consume)])],
    )
    _run(pipeline, tmp_path, storage=storage)
    assert storage.results(pipeline, job_id="j1").of("consume")["doc-1"] == {"consumed": "doc-1"}


# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------


def test_the_step_decorator_still_attaches_a_dataset(tmp_path: Path) -> None:
    @step(output=TextDataset(name="rendered"))
    def render(document: Any, *, context: Any) -> Any:
        return f"id={document['id']}"

    storage = DurableStorage.at(tmp_path)
    pipeline = Pipeline(name="p", stages=[Stage(name="s", entries=[render])])
    _run(pipeline, tmp_path, storage=storage)
    assert storage.results(pipeline, job_id="j1").of("render")["doc-1"] == "id=doc-1"


def test_an_explicit_dataset_overrides_the_default(tmp_path: Path) -> None:
    def render(document: Any, *, context: Any) -> Any:
        return f"id={document['id']}"

    declared = Step(render, output=TextDataset(name="text"))
    assert isinstance(declared.dataset, TextDataset)
    assert Step(render).dataset.name == "render"
    assert isinstance(Step(render).dataset, JSONDataset)


def test_persist_false_is_rejected_because_recovery_needs_committed_output() -> None:
    def render(document: Any, *, context: Any) -> Any:
        return document

    with pytest.raises(ValueError, match="persist=False"):
        Step(render, persist=False)


def test_a_pipeline_may_declare_its_own_source(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="p",
        stages=[Stage(name="s", entries=[Step(lambda d, *, context: d)])],
        source=InMemorySource(DOCUMENTS),
    )
    pipeline.stages[0].entries[0].name = "identity"
    result = run(pipeline, job_id="j1", storage=DurableStorage.at(tmp_path))
    assert result.status == "completed"


def test_a_loader_is_accepted_as_a_source(tmp_path: Path) -> None:
    """Loaders describe input; they are not a second execution path."""
    from caravel import CallableLoader

    storage = DurableStorage.at(tmp_path)
    pipeline = Pipeline(name="p", stages=[Stage(name="s", entries=[Step(lambda d, *, context: d)])])
    pipeline.stages[0].entries[0].name = "identity"
    result = run(
        pipeline,
        job_id="j1",
        source=CallableLoader("src", lambda: DOCUMENTS),
        storage=storage,
    )
    assert result.status == "completed"
    assert set(storage.results(pipeline, job_id="j1").of("identity")) == set(DOCUMENTS)


def test_running_without_any_source_is_rejected(tmp_path: Path) -> None:
    from caravel import SourceError

    pipeline = Pipeline(name="p", stages=[Stage(name="s", entries=[Step(lambda d, *, context: d)])])
    pipeline.stages[0].entries[0].name = "identity"
    with pytest.raises(SourceError, match="No input source"):
        run(pipeline, job_id="j1", storage=DurableStorage.at(tmp_path))


# ---------------------------------------------------------------------------
# Source tagging
# ---------------------------------------------------------------------------


def test_the_source_tag_is_stripped_before_output_is_committed(tmp_path: Path) -> None:
    """Preparing a payload happens before the commit, so immutability is intact."""
    from caravel import SOURCE_FIELD, CallableLoader

    def passthrough(record: Any, *, context: Any) -> Any:
        assert SOURCE_FIELD in record, "steps still see the tag"
        return record

    storage = DurableStorage.at(tmp_path)
    pipeline = Pipeline(name="p", stages=[Stage(name="s", entries=[Step(passthrough)])])
    run(
        pipeline,
        job_id="j1",
        source=CallableLoader("src", lambda: {"p1": {SOURCE_FIELD: "a", "v": 1}}),
        storage=storage,
    )
    committed = storage.results(pipeline, job_id="j1").of("passthrough")["p1"]
    assert SOURCE_FIELD not in committed, "lineage tag leaked into committed output"
    assert committed == {"v": 1}


def test_keep_source_tag_retains_it_when_asked(tmp_path: Path) -> None:
    from caravel import SOURCE_FIELD, CallableLoader

    storage = DurableStorage.at(tmp_path)
    pipeline = Pipeline(
        name="p",
        stages=[Stage(name="s", entries=[Step(lambda r, *, context: r)])],
    )
    pipeline.stages[0].entries[0].name = "passthrough"
    run(
        pipeline,
        job_id="j1",
        source=CallableLoader("src", lambda: {"p1": {SOURCE_FIELD: "a", "v": 1}}),
        storage=storage,
        keep_source_tag=True,
    )
    committed = storage.results(pipeline, job_id="j1").of("passthrough")["p1"]
    assert committed[SOURCE_FIELD] == "a"
