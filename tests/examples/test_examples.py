"""The shipped examples run end to end on the one durable execution model.

These replace the per-example suites that asserted the retired stage-directory
layout. Committed output is now read through ``storage.results(...)``, which is
the supported way to read a job's output.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from caravel import DurableStorage  # noqa: E402
from examples.combining_results.pipeline import (  # noqa: E402
    build_combining_pipeline,
    run_combining_pipeline,
)
from examples.custom_context.pipeline import (  # noqa: E402
    build_custom_context_pipeline,
    run_custom_context_pipeline,
)
from examples.fsspec_minimal.pipeline import (  # noqa: E402
    build_fsspec_pipeline,
    run_fsspec_pipeline,
)
from examples.fsspec_partitioned.pipeline import (  # noqa: E402
    build_fsspec_partitioned_pipeline,
    run_fsspec_partitioned_pipeline,
)
from examples.minimal.pipeline import (  # noqa: E402
    build_minimal_pipeline,
    run_minimal_pipeline,
)
from examples.multisource.pipeline import (  # noqa: E402
    build_multisource_pipeline,
    run_multisource_pipeline,
)
from examples.partitioned.pipeline import (  # noqa: E402
    build_partitioned_pipeline,
    run_partitioned_pipeline,
)
from examples.persist_only_final.pipeline import (  # noqa: E402
    build_persist_only_final_pipeline,
    run_persist_only_final_pipeline,
)


def _results(tmp_path: Path, pipeline: Any, job_id: str) -> Any:
    return DurableStorage.at(tmp_path / "output").results(pipeline, job_id=job_id)


# ---------------------------------------------------------------------------
# Each example completes and commits readable output
# ---------------------------------------------------------------------------


def test_minimal_example_keeps_results_per_partition(tmp_path: Path) -> None:
    result = run_minimal_pipeline(state_root=tmp_path)
    assert result.status == "completed"
    assert result.restart_safe is True

    results = _results(tmp_path, build_minimal_pipeline(), "minimal-1")
    per_partition = results.of("silver_transform")
    assert {key: records[key]["normalized"] for key, records in per_partition.items()} == {
        "alpha": 1.0,
        "beta": 3.0,
    }
    assert set(results.of("bronze_collect")) == set(per_partition)
    steps = [step for stage in build_minimal_pipeline().stages for step in stage.entries]
    assert [step.name for step in steps] == ["bronze_collect", "silver_transform"]
    assert all(step.input_scope == "partition" for step in steps)


def test_combining_example_aggregates_all_partition_results(tmp_path: Path) -> None:
    assert run_combining_pipeline(state_root=tmp_path).status == "completed"
    results = _results(tmp_path, build_combining_pipeline(), "combining-1")
    per_partition = results.of("transform")
    summary = list(results.of("combine_results").values())

    assert len(summary) == 1, "the barrier produces one aggregate partition"
    assert summary[0]["count"] == len(per_partition)
    assert summary[0]["ids"] == ["alpha", "beta"]
    assert summary[0]["total_normalized"] == 4.0
    assert summary[0]["records"] == {
        key: record for payload in per_partition.values() for key, record in payload.items()
    }


@pytest.mark.parametrize(
    ("example", "stage_index"),
    [
        (name, index)
        for name in (
            "minimal",
            "branching",
            "partitioned",
            "fsspec_partitioned",
            "combining_results",
        )
        for index in range(
            1
            if name == "combining_results"
            else 2
            if name in ("minimal", "fsspec_partitioned")
            else 3
        )
    ],
)
def test_collection_examples_preserve_records_when_scope_changes(
    tmp_path: Path, example: str, stage_index: int
) -> None:
    from caravel import run
    from examples.branching.pipeline import build_branching_pipeline

    builders = {
        "minimal": build_minimal_pipeline,
        "branching": build_branching_pipeline,
        "partitioned": build_partitioned_pipeline,
        "fsspec_partitioned": build_fsspec_partitioned_pipeline,
        "combining_results": build_combining_pipeline,
    }

    def execute(scope: str) -> dict[str, Any]:
        pipeline = builders[example]()
        pipeline.stages[stage_index].entries[-1].input_scope = scope
        final = pipeline.stages[-1].entries[-1]
        with DurableStorage.at(tmp_path / scope / "output") as storage:
            assert run(pipeline, job_id="scope", storage=storage).status == "completed"
            payloads = storage.results(pipeline, job_id="scope").of(final.name).values()
            if example in ("fsspec_partitioned", "combining_results"):
                return next(iter(payloads))
            merged = {}
            for payload in payloads:
                assert not merged.keys() & payload.keys()
                merged.update(payload)
            return merged

    assert execute("partition") == execute("all")


@pytest.mark.parametrize(
    ("example", "step_name"),
    [
        ("fsspec", "bronze_collect"),
        ("fsspec", "silver_transform"),
        ("persist", "step_1_collect"),
        ("persist", "step_2_scale"),
        ("persist", "step_3_label"),
        ("durable", "enrich"),
        ("durable", "publish"),
    ],
)
def test_remaining_collection_examples_process_multiple_inputs(
    tmp_path: Path, example: str, step_name: str
) -> None:
    from caravel import run
    from caravel.durability import InMemorySource
    from examples.durable_partitions.pipeline import DOCUMENTS, build_pipeline

    builders = {
        "fsspec": build_fsspec_pipeline,
        "persist": build_persist_only_final_pipeline,
        "durable": build_pipeline,
    }

    def execute(scope: str) -> Any:
        pipeline = builders[example]()
        for stage in pipeline.stages:
            for step in stage.entries:
                if step.name == step_name:
                    step.input_scope = scope
        with DurableStorage.at(tmp_path / scope / "output") as storage:
            source = InMemorySource(DOCUMENTS) if example == "durable" else pipeline.source
            assert (
                run(pipeline, job_id="loop", source=source, storage=storage).status == "completed"
            )
            final_step = pipeline.stages[-1].entries[-1]
            return storage.results(pipeline, job_id="loop").of(final_step.name)

    assert execute("all") == execute("partition")


def test_partitioned_example_keeps_storage_keys_distinct_from_identity(
    tmp_path: Path,
) -> None:
    assert run_partitioned_pipeline(state_root=tmp_path).status == "completed"

    results = _results(tmp_path, build_partitioned_pipeline(), "partitioned-1")
    gold = results.of("gold_partition_by_language")
    assert gold, "the gold step committed nothing"
    # Nested "<lang>/<id>" keys live inside a payload; execution identity is
    # the partition id, a separate namespace.
    nested = [key for payload in gold.values() for key in payload]
    assert any("/" in key for key in nested)


def test_persist_only_final_example_commits_every_step(tmp_path: Path) -> None:
    assert run_persist_only_final_pipeline(state_root=tmp_path).status == "completed"

    results = _results(tmp_path, build_persist_only_final_pipeline(), "persist-only-final-1")
    # Every step commits now: recovery restores output instead of recomputing.
    for name in ("step_1_collect", "step_2_scale", "step_3_label"):
        assert results.of(name), f"{name} committed nothing"
    aggregate = list(results.of("step_4_aggregate").values())
    assert len(aggregate) == 1
    assert aggregate[0]["count"] == len(results.of("step_3_label"))


def test_multisource_example_preserves_source_tags(tmp_path: Path) -> None:
    assert run_multisource_pipeline(state_root=tmp_path).status == "completed"

    results = _results(tmp_path, build_multisource_pipeline(), "multisource-1")
    summary = list(results.of("summarize_by_source").values())
    assert len(summary) == 1
    counts = summary[0]["counts_by_source"]
    assert sum(counts.values()) == summary[0]["total_records"]
    assert all(count > 0 for count in counts.values())


def test_custom_context_example_wraps_the_framework_context(tmp_path: Path) -> None:
    assert run_custom_context_pipeline(state_root=tmp_path).status == "completed"

    results = _results(tmp_path, build_custom_context_pipeline(), "custom-context-1")
    summary = list(results.of("summarize").values())[0]
    assert summary["job_name"] == "CUSTOM_CONTEXT_EXAMPLE"
    assert summary["count"] == 2


# ---------------------------------------------------------------------------
# Remote-capable artifact storage
# ---------------------------------------------------------------------------


def test_fsspec_example_runs_with_local_artifacts(tmp_path: Path) -> None:
    assert run_fsspec_pipeline(state_root=tmp_path).status == "completed"
    results = _results(tmp_path, build_fsspec_pipeline(), "fsspec-minimal-1")
    assert list(results.of("silver_finalize").values())[0]["count"] > 0


@pytest.mark.parametrize("artifacts_root", ["memory://fsspec-minimal"])
def test_fsspec_example_runs_with_remote_artifacts(tmp_path: Path, artifacts_root: str) -> None:
    """Authoritative state stays local; committed artifacts go elsewhere."""
    result = run_fsspec_pipeline(state_root=tmp_path, artifacts_root=artifacts_root)
    assert result.status == "completed"

    storage = DurableStorage.at(tmp_path / "output", artifacts_root=artifacts_root)
    summary = storage.results(build_fsspec_pipeline(), job_id="fsspec-minimal-1").of(
        "silver_finalize"
    )
    assert list(summary.values())[0]["count"] > 0
    assert not (tmp_path / "artifacts").exists(), "artifacts were written locally"


def test_fsspec_partitioned_example_runs_with_remote_artifacts(
    tmp_path: Path,
) -> None:
    artifacts_root = "memory://fsspec-partitioned"
    result = run_fsspec_partitioned_pipeline(state_root=tmp_path, artifacts_root=artifacts_root)
    assert result.status == "completed"

    storage = DurableStorage.at(tmp_path / "output", artifacts_root=artifacts_root)
    summary = storage.results(
        build_fsspec_partitioned_pipeline(), job_id="fsspec-partitioned-1"
    ).of("gold_summarize")
    aggregate = list(summary.values())[0]
    assert aggregate["total_records"] > 0
    assert aggregate["counts_by_language"]


# ---------------------------------------------------------------------------
# Restart behaviour, shared by every example
# ---------------------------------------------------------------------------


def test_reinvoking_an_example_reprocesses_nothing(tmp_path: Path) -> None:
    first = run_minimal_pipeline(state_root=tmp_path)
    assert first.status == "completed"
    again = run_minimal_pipeline(state_root=tmp_path)
    assert again.status == "completed"
    assert again.committed_batches == 0


def test_the_production_profile_composes_storage_policy_and_observers(
    tmp_path: Path,
) -> None:
    from examples.production_profile.profile import (
        build_production_plugins,
        build_production_policy,
        build_production_storage,
    )

    storage = build_production_storage(tmp_path / "state")
    assert storage.restart_safe is True

    plugins = build_production_plugins(tmp_path / "metadata")
    assert len(plugins) == 1, "only observer evidence remains a plugin concern"

    assert build_production_policy().workers == 1
    concurrent = build_production_policy(workers=4)
    assert concurrent.workers == 4 and concurrent.leases is not None
