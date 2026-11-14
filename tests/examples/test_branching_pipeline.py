"""The branching example: one route per job, merged into one result."""

from __future__ import annotations

from pathlib import Path

import pytest

from caravel import PartitionedJSONDataset
from examples.branching.pipeline import (
    HTML_SOURCE_NAME,
    JSON_SOURCE_NAME,
    run_branching_pipeline,
)


def _gold(result):
    return PartitionedJSONDataset().load_from(result.outputs["gold_partition_by_language"])


@pytest.mark.parametrize(
    "source_name, route_step",
    [
        (JSON_SOURCE_NAME, "bronze_normalize_json_source"),
        (HTML_SOURCE_NAME, "bronze_normalize_html_source"),
    ],
)
def test_each_source_runs_its_own_route(tmp_path: Path, source_name: str, route_step: str) -> None:
    result = run_branching_pipeline(
        state_root=tmp_path, source_name=source_name, job_id=f"branch-{source_name}"
    )
    assert route_step in result.outputs
    assert "branch_by_source_merge" in result.outputs
    assert _gold(result)


def test_both_sources_coexist_in_one_pipeline_database(tmp_path: Path) -> None:
    import sqlite3

    for source_name in (JSON_SOURCE_NAME, HTML_SOURCE_NAME):
        run_branching_pipeline(
            state_root=tmp_path, source_name=source_name, job_id=f"branch-{source_name}"
        )

    for source_name in (JSON_SOURCE_NAME, HTML_SOURCE_NAME):
        database = tmp_path / "output" / ".caravel" / "state.sqlite3"
        assert database.exists()
        connection = sqlite3.connect(database)
        jobs = {row[0] for row in connection.execute("SELECT job_id FROM runs")}
        connection.close()
        assert f"branch-{source_name}" in jobs


def test_the_unchosen_route_is_recorded_as_skipped(tmp_path: Path) -> None:
    import sqlite3

    run_branching_pipeline(state_root=tmp_path, source_name=JSON_SOURCE_NAME, job_id="branch-json")
    connection = sqlite3.connect(tmp_path / "output" / ".caravel" / "state.sqlite3")
    statuses = dict(connection.execute("SELECT step_name, status FROM steps"))
    connection.close()

    assert statuses["bronze_normalize_json_source"] == "completed"
    assert statuses["bronze_normalize_html_source"] == "skipped"


def test_reopening_a_source_job_restores_its_steps(tmp_path: Path) -> None:
    first = run_branching_pipeline(
        state_root=tmp_path, source_name=JSON_SOURCE_NAME, job_id="branch-json"
    )
    second = run_branching_pipeline(
        state_root=tmp_path, source_name=JSON_SOURCE_NAME, job_id="branch-json"
    )
    assert first.steps_executed > 0
    assert second.steps_executed == 0
    assert second.steps_restored == first.steps_executed
