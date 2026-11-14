"""The durability demo: what survives an interruption, and what runs again."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from examples.durable_partitions import pipeline as demo
from examples.durable_partitions.pipeline import (
    DOCUMENTS,
    build_pipeline,
    run_demo,
    run_to_completion,
)


@pytest.fixture(autouse=True)
def _instant_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the demo's per-document sleep out of the test suite."""
    monkeypatch.setattr(demo, "WORK_SECONDS", 0.0)


def _statuses(root: Path) -> dict[str, str]:
    connection = sqlite3.connect(root / ".caravel" / "state.sqlite3")
    try:
        return dict(connection.execute("SELECT step_name, status FROM steps"))
    finally:
        connection.close()


def test_the_demo_summarizes_every_document(tmp_path: Path) -> None:
    outcome = run_to_completion(tmp_path)
    assert outcome["summary"]["documents"] == len(DOCUMENTS)
    assert outcome["summary"]["words"] == sum(d["words"] for d in DOCUMENTS.values())


def test_reopening_the_job_restores_its_committed_steps(tmp_path: Path) -> None:
    outcome = run_to_completion(tmp_path)
    assert outcome["steps_executed"] == 3
    assert outcome["steps_restored"] == 3


def test_an_interrupted_run_keeps_the_steps_that_committed(tmp_path: Path) -> None:
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="publish"):
        run_demo(output=output, job_id="demo-1", fail_at="publish")

    statuses = _statuses(tmp_path)
    assert statuses["enrich"] == "completed"
    assert statuses["publish"] == "pending"

    result = run_demo(output=output, job_id="demo-1")
    assert result.steps_restored == 1
    assert result.steps_executed == 2


def test_pipeline_scope_commits_once_on_success(tmp_path: Path) -> None:
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="summarize"):
        run_demo(output=output, job_id="demo-2", commit_scope="pipeline", fail_at="summarize")

    # The run committed once, on success, so the failed attempt left no rows.
    assert _statuses(tmp_path) == {}

    result = run_demo(output=output, job_id="demo-2", commit_scope="pipeline")
    assert result.steps_executed == 3
    assert result.steps_restored == 0


def test_step_scope_keeps_each_completed_step(tmp_path: Path) -> None:
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="summarize"):
        run_demo(output=output, job_id="demo-3", commit_scope="step", fail_at="summarize")

    result = run_demo(output=output, job_id="demo-3", commit_scope="step")
    assert result.steps_restored == 2
    assert result.steps_executed == 1


def test_a_transient_failure_is_retried_before_anything_commits(tmp_path: Path) -> None:
    output = tmp_path / "output"
    result = run_demo(output=output, job_id="demo-4", flaky_attempts=2)

    assert result.steps_executed == 3
    assert _statuses(tmp_path)["publish"] == "completed"


def test_a_run_without_a_job_id_keeps_no_database(tmp_path: Path) -> None:
    result = run_demo(output=tmp_path / "output")
    assert result.job_id is None
    assert not (tmp_path / ".caravel" / "state.sqlite3").exists()


def test_deleted_output_is_regenerated_from_snapshots(tmp_path: Path) -> None:
    output = tmp_path / "output"
    run_demo(output=output, job_id="demo-5")
    written = sorted(output.rglob("*.json"))
    assert written

    for path in written:
        path.unlink()

    result = run_demo(output=output, job_id="demo-5")
    assert result.steps_executed == 0
    assert result.steps_restored == 3
    assert sorted(output.rglob("*.json")) == written


def test_the_declaration_names_its_stages_in_order() -> None:
    assert [stage.name for stage in build_pipeline().stages] == [
        "prepare",
        "publish",
        "summary",
    ]
