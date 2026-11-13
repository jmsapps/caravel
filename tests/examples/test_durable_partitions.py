"""The durable partitions example runs end to end across bounded invocations."""

from __future__ import annotations

from pathlib import Path

from examples.durable_partitions.pipeline import (
    DOCUMENTS,
    PIPELINE_ID,
    run_to_completion,
)


def test_the_example_completes_and_publishes_every_document(tmp_path: Path) -> None:
    summary = run_to_completion(tmp_path)

    assert summary["status"] == "completed"
    assert summary["invocations"] >= 1
    assert sorted(set(summary["consumed_before_completion"])) == sorted(DOCUMENTS)


def test_the_example_produces_a_summary_behind_its_barrier(tmp_path: Path) -> None:
    from caravel import DurableStorage

    from examples.durable_partitions.pipeline import build_pipeline

    run_to_completion(tmp_path)

    storage = DurableStorage.at(tmp_path / "output")
    try:
        job = storage.progress.load_job("snapshot-v1")
        assert job is not None and job.status == "completed"

        summary = storage.results(build_pipeline(), job_id="snapshot-v1").of("summarize")
        expected_words = sum(document["words"] for document in DOCUMENTS.values())
        assert list(summary.values()) == [{"documents": len(DOCUMENTS), "words": expected_words}]
    finally:
        storage.close()


def test_the_example_reinvokes_without_reprocessing(tmp_path: Path) -> None:
    first = run_to_completion(tmp_path)
    assert first["status"] == "completed"

    second = run_to_completion(tmp_path)
    assert second["status"] == "completed"
    assert second["invocations"] == 1
    assert second["consumed_before_completion"] == []


def test_the_example_identifiers_stay_stable() -> None:
    """The consumer addresses a declared step, and the node id behind it is stable."""
    from caravel.durability import compile_durable_plan

    from examples.durable_partitions.pipeline import build_pipeline

    assert PIPELINE_ID == "examples/durable-partitions"
    plan = compile_durable_plan(build_pipeline())
    publish = next(node for node in plan.nodes if node.step.name == "publish")
    assert publish.node_id == "stage-002-entry-001"
