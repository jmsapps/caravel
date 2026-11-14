"""The example pipelines, read through the result each run returns."""

from __future__ import annotations

from pathlib import Path

import pytest

from caravel import JSONDataset, PartitionedJSONDataset


def _json(result, step: str):
    return JSONDataset().load_from(result.outputs[step])


def _partitioned(result, step: str):
    return PartitionedJSONDataset().load_from(result.outputs[step])


def test_the_minimal_example_normalizes_every_record(tmp_path: Path) -> None:
    import json

    from examples.minimal.pipeline import _DEFAULT_INPUT_PATH, run_minimal_pipeline

    seed = json.loads(Path(_DEFAULT_INPUT_PATH).read_text())
    result = run_minimal_pipeline(state_root=tmp_path)
    per_record = _partitioned(result, "silver_transform")

    # bronze doubles each value; silver divides the doubled value by four.
    expected = {key: (record["value"] * 2) / 4.0 for key, record in seed.items()}
    assert {key: record["normalized"] for key, record in per_record.items()} == expected
    assert set(_partitioned(result, "bronze_collect")) == set(per_record)


def test_the_combining_example_aggregates_every_record(tmp_path: Path) -> None:
    from examples.combining_results.pipeline import run_combining_pipeline

    result = run_combining_pipeline(state_root=tmp_path)
    per_record = _partitioned(result, "transform")
    summary = _json(result, "combine_results")

    assert summary["count"] == len(per_record)
    assert summary["ids"] == ["alpha", "beta"]
    assert summary["total_normalized"] == 4.0
    assert summary["records"] == per_record


def test_the_partitioned_example_repartitions_by_language(tmp_path: Path) -> None:
    from examples.partitioned.pipeline import run_partitioned_pipeline

    result = run_partitioned_pipeline(state_root=tmp_path)
    gold = _partitioned(result, "gold_partition_by_language")

    assert gold
    assert any("/" in key for key in gold)


def test_persist_only_final_commits_every_step(tmp_path: Path) -> None:
    from examples.persist_only_final.pipeline import run_persist_only_final_pipeline

    result = run_persist_only_final_pipeline(state_root=tmp_path)
    for step in ("step_1_collect", "step_2_scale", "step_3_label"):
        assert _json(result, step)

    summary = _json(result, "step_4_aggregate")
    assert summary["count"] == len(_json(result, "step_3_label"))


def test_the_multisource_example_keeps_every_source(tmp_path: Path) -> None:
    from examples.multisource.pipeline import run_multisource_pipeline

    result = run_multisource_pipeline(state_root=tmp_path)
    summary = _json(result, "summarize_by_source")
    counts = summary["counts_by_source"]

    assert sum(counts.values()) == summary["total_records"]
    assert all(count > 0 for count in counts.values())


def test_the_custom_context_example_exposes_its_wrapper(tmp_path: Path) -> None:
    from examples.custom_context.pipeline import run_custom_context_pipeline

    summary = _json(run_custom_context_pipeline(state_root=tmp_path), "summarize")
    assert summary["job_name"] == "CUSTOM_CONTEXT_EXAMPLE"
    assert summary["count"] == 2


def test_the_fsspec_example_runs_against_local_storage(tmp_path: Path) -> None:
    from examples.fsspec_minimal.pipeline import run_fsspec_pipeline

    result = run_fsspec_pipeline(state_root=tmp_path)
    assert _json(result, "silver_finalize")["count"] > 0


def test_the_fsspec_partitioned_example_summarizes_every_language(tmp_path: Path) -> None:
    from examples.fsspec_partitioned.pipeline import run_fsspec_partitioned_pipeline

    summary = _json(run_fsspec_partitioned_pipeline(state_root=tmp_path), "gold_summarize")
    assert summary["total_records"] > 0
    assert summary["counts_by_language"]


def test_a_rerun_without_durability_executes_again(tmp_path: Path) -> None:
    from examples.minimal.pipeline import build_minimal_pipeline
    from caravel import run

    pipeline = build_minimal_pipeline()
    first = run(pipeline, output=tmp_path / "output")
    second = run(pipeline, output=tmp_path / "output")

    assert first.steps_executed == second.steps_executed == 2
    assert second.steps_restored == 0


def test_the_production_profile_states_three_decisions(tmp_path: Path) -> None:
    from examples.production_profile.profile import (
        build_production_durability,
        build_production_output,
        build_production_plugins,
    )

    assert build_production_output(tmp_path).name == "output"
    options = build_production_durability("job-1")
    assert options.job_id == "job-1"
    assert options.commit_scope == "step"
    assert len(build_production_plugins(tmp_path / "metadata")) == 1


@pytest.mark.parametrize(
    "module, entry",
    [
        ("examples.minimal.pipeline", "run_minimal_pipeline"),
        ("examples.combining_results.pipeline", "run_combining_pipeline"),
        ("examples.partitioned.pipeline", "run_partitioned_pipeline"),
        ("examples.persist_only_final.pipeline", "run_persist_only_final_pipeline"),
        ("examples.custom_context.pipeline", "run_custom_context_pipeline"),
        ("examples.multisource.pipeline", "run_multisource_pipeline"),
    ],
)
def test_a_durable_example_restores_its_committed_steps(
    tmp_path: Path, module: str, entry: str
) -> None:
    import importlib

    runner = getattr(importlib.import_module(module), entry)
    first = runner(state_root=tmp_path)
    second = runner(state_root=tmp_path)

    assert first.steps_executed > 0
    assert second.steps_executed == 0
    assert second.steps_restored == first.steps_executed


def test_each_example_keeps_its_output_in_its_own_data_folder() -> None:
    """An example run writes beside the example, never in the repository root."""
    from examples.support import example_data_dir, example_output

    module = "/repo/examples/widget/__main__.py"
    assert example_data_dir(module).name == "data"
    assert example_data_dir(module).parent.name == "widget"
    assert example_output(module).name == "output"
    assert example_output(module).parent == example_data_dir(module)


def test_the_cli_default_output_is_configurable(tmp_path: Path) -> None:
    from caravel import InMemorySource, Pipeline, Stage, Step, make_cli

    def produce(value, *, context):
        return {"n": 1}

    pipeline = Pipeline(
        name="widget",
        stages=[Stage(name="only", entries=[Step(produce, name="produce")])],
        source=InMemorySource({"a": 1}),
    )
    chosen = tmp_path / "chosen" / "output"
    assert make_cli(pipeline, default_output=chosen)([]) == 0
    assert sorted(chosen.rglob("*.json"))
