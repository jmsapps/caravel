"""Ordered execution over complete values."""

from __future__ import annotations

from pathlib import Path

from caravel import (
    SOURCE_FIELD,
    CallableLoader,
    DurabilityOptions,
    InMemorySource,
    JSONDataset,
    MultiSourceLoader,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)


def build(entries, *, source=None, durable=False, job_id="exec-1", stage_name="only"):
    return Pipeline(
        name="exec_example",
        stages=[Stage(name=stage_name, entries=entries)],
        source=source or InMemorySource({"a": 1}),
        durability_options=(
            DurabilityOptions(schema_version="v1.0.0", job_id=job_id, commit_scope="step")
            if durable
            else None
        ),
    )


def test_a_processor_receives_the_complete_upstream_value(tmp_path: Path) -> None:
    seen = []

    def first(value, *, context):
        return {"doubled": {key: item * 2 for key, item in value.items()}}

    def second(value, *, context):
        seen.append(value)
        return value

    run(
        build([Step(first), Step(second)], source=InMemorySource({"a": 1, "b": 2})),
        output=tmp_path / "output",
    )
    assert seen == [{"doubled": {"a": 2, "b": 4}}]


def test_steps_run_in_declaration_order(tmp_path: Path) -> None:
    order = []

    def first(value, *, context):
        order.append("first")
        return value

    def second(value, *, context):
        order.append("second")
        return value

    pipeline = Pipeline(
        name="ordered",
        stages=[
            Stage(name="one", entries=[Step(first, name="first")]),
            Stage(name="two", entries=[Step(second, name="second")]),
        ],
        source=InMemorySource({"a": 1}),
    )
    run(pipeline, output=tmp_path / "output")
    assert order == ["first", "second"]


def test_a_bare_callable_is_a_step(tmp_path: Path) -> None:
    def produce(value, *, context):
        return {"n": 1}

    result = run(build([produce]), output=tmp_path / "output")
    assert "produce" in result.outputs


def test_persist_false_passes_the_value_without_saving_it(tmp_path: Path) -> None:
    def hidden(value, *, context):
        return {"hidden": True}

    def visible(value, *, context):
        return value

    result = run(
        build([Step(hidden, persist=False), Step(visible, name="visible")]),
        output=tmp_path / "output",
    )
    assert "hidden" not in result.outputs
    assert JSONDataset().load_from(result.outputs["visible"]) == {"hidden": True}


def test_the_context_carries_the_declaration_facts(tmp_path: Path) -> None:
    seen = {}

    def inspect(value, *, context):
        seen.update(
            {
                "pipeline_name": context.pipeline_name,
                "stage_name": context.stage_name,
                "step_name": context.step_name,
                "params": dict(context.params),
            }
        )
        return value

    run(
        build([Step(inspect, name="inspect")], stage_name="bronze"),
        output=tmp_path / "output",
        params={"region": "eu"},
    )
    assert seen["pipeline_name"] == "exec_example"
    assert seen["stage_name"] == "bronze"
    assert seen["step_name"] == "inspect"
    assert seen["params"] == {"region": "eu"}


def test_a_context_factory_wraps_the_framework_context(tmp_path: Path) -> None:
    class JobContext:
        def __init__(self, base):
            self.base = base
            self.job_name = base.pipeline_name.upper()

    seen = []

    def inspect(value, *, context):
        seen.append(context.job_name)
        return value

    run(
        build([Step(inspect)]),
        output=tmp_path / "output",
        context_factory=JobContext,
    )
    assert seen == ["EXEC_EXAMPLE"]


def _multi_source():
    return MultiSourceLoader(
        [
            CallableLoader("feed_a", lambda: {"one": {"value": 1}}),
            CallableLoader("feed_b", lambda: {"two": {"value": 2}}),
        ]
    )


def test_source_tags_reach_the_processor(tmp_path: Path) -> None:
    seen = []

    def inspect(value, *, context):
        seen.append({key: item[SOURCE_FIELD] for key, item in value.items()})
        return {"count": len(value)}

    run(build([Step(inspect)], source=_multi_source()), output=tmp_path / "output")
    assert seen == [{"one": "feed_a", "two": "feed_b"}]


def test_a_source_tag_is_stripped_from_committed_output(tmp_path: Path) -> None:
    def passthrough(value, *, context):
        return value

    result = run(
        build([Step(passthrough, output=PartitionedJSONDataset())], source=_multi_source()),
        output=tmp_path / "output",
    )
    saved = PartitionedJSONDataset().load_from(result.outputs["passthrough"])
    assert all(SOURCE_FIELD not in record for record in saved.values())


def test_keeping_the_source_tag_is_a_run_decision(tmp_path: Path) -> None:
    def passthrough(value, *, context):
        return value

    result = run(
        build([Step(passthrough, output=PartitionedJSONDataset())], source=_multi_source()),
        output=tmp_path / "output",
        keep_source_tag=True,
    )
    saved = PartitionedJSONDataset().load_from(result.outputs["passthrough"])
    assert all(SOURCE_FIELD in record for record in saved.values())


def test_a_run_reports_what_it_executed_and_where(tmp_path: Path) -> None:
    def produce(value, *, context):
        return {"n": 1}

    result = run(build([Step(produce, name="produce")]), output=tmp_path / "output")
    assert result.pipeline_name == "exec_example"
    assert result.steps_executed == 1
    assert result.steps_restored == 0
    assert Path(result.outputs["produce"]).exists()
