"""The CLI drives the one execution model."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from caravel import InMemorySource, JSONDataset, Pipeline, Stage, Step, make_cli

DOCUMENTS = {"doc-1": {"id": "doc-1"}, "doc-2": {"id": "doc-2"}}


def load(value, *, context: Any) -> Any:
    return {key: {**record, "loaded": True} for key, record in value.items()}


def _pipeline() -> Pipeline:
    return Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(load, output=JSONDataset(name="load"))])],
        source=InMemorySource(DOCUMENTS),
    )


def test_a_run_reports_what_it_did_and_where(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli = make_cli(_pipeline())
    assert cli(["--output", str(tmp_path / "output")]) == 0
    out = capsys.readouterr().out
    assert "pipeline=documents" in out
    assert "executed=1" in out
    assert f"output={tmp_path / 'output'}" in out


def test_a_job_id_makes_the_run_durable(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = tmp_path / "output"
    assert make_cli(_pipeline())(["--output", str(output), "--job-id", "j1"]) == 0
    assert "job_id=j1" in capsys.readouterr().out
    assert (tmp_path / ".caravel" / "state.sqlite3").exists()


def test_resuming_the_same_job_restores_committed_steps(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "output"
    make_cli(_pipeline())(["--output", str(output), "--job-id", "j1"])
    capsys.readouterr()

    assert make_cli(_pipeline())(["--output", str(output), "--job-id", "j1"]) == 0
    out = capsys.readouterr().out
    assert "executed=0" in out
    assert "restored=1" in out


def test_the_commit_scope_is_selectable(tmp_path: Path) -> None:
    import sqlite3

    output = tmp_path / "output"
    cli = make_cli(_pipeline())
    assert cli(["--output", str(output), "--job-id", "j1", "--commit-scope", "stage"]) == 0

    connection = sqlite3.connect(tmp_path / ".caravel" / "state.sqlite3")
    scope = connection.execute("SELECT commit_scope FROM runs").fetchone()[0]
    connection.close()
    assert scope == "stage"


def test_graph_prints_the_pipeline_without_running(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert make_cli(_pipeline())(["--graph"]) == 0
    assert "flowchart" in capsys.readouterr().out.lower()


def test_params_reach_the_run(tmp_path: Path) -> None:
    seen: dict[str, str] = {}

    def capture(value: Any, *, context: Any) -> Any:
        seen.update(dict(context.params))
        return value

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(capture)])],
        source=InMemorySource(DOCUMENTS),
    )
    cli = make_cli(pipeline)
    assert cli(["--output", str(tmp_path / "output"), "--param", "region=eu-west-1"]) == 0
    assert seen == {"region": "eu-west-1"}


def test_a_param_value_may_contain_an_equals_sign(tmp_path: Path) -> None:
    seen: dict[str, str] = {}

    def capture(value: Any, *, context: Any) -> Any:
        seen.update(dict(context.params))
        return value

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(capture)])],
        source=InMemorySource(DOCUMENTS),
    )
    cli = make_cli(pipeline)
    cli(["--output", str(tmp_path / "output"), "--param", "token=a=b"])
    assert seen == {"token": "a=b"}


@pytest.mark.parametrize("value", ["novalue", "=empty"])
def test_a_param_states_a_key_and_a_value(tmp_path: Path, value: str) -> None:
    cli = make_cli(_pipeline())
    with pytest.raises(SystemExit):
        cli(["--output", str(tmp_path / "output"), "--param", value])


def test_observers_passed_to_the_cli_reach_the_run(tmp_path: Path) -> None:
    kinds: list[str] = []

    class Recorder:
        plugin_id = "recorder"
        criticality = "best_effort"

        def on_event(self, event: Any) -> None:
            kinds.append(event.kind)

    cli = make_cli(_pipeline(), plugins=[Recorder()])
    assert cli(["--output", str(tmp_path / "output")]) == 0
    assert kinds[0] == "run_started"
    assert kinds[-1] == "run_completed"


def test_committed_output_is_readable_after_a_cli_run(tmp_path: Path) -> None:
    output = tmp_path / "output"
    assert make_cli(_pipeline())(["--output", str(output), "--job-id", "j1"]) == 0

    written = sorted(output.rglob("*.json"))
    assert written
    payload = json.loads(written[0].read_text())
    assert payload["doc-1"]["loaded"] is True
