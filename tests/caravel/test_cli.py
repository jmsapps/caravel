"""The CLI drives the one durable execution model."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from caravel import InMemorySource, JSONDataset, Pipeline, Stage, Step, make_cli

DOCUMENTS = {"doc-1": {"id": "doc-1"}, "doc-2": {"id": "doc-2"}}


def load_partition(partition, *, context: Any) -> Any:
    document: Any = partition.load()
    return {**document, "loaded": True}


def load(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(load_partition)


def _pipeline() -> Pipeline:
    return Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(load, output=JSONDataset(name="load"))])],
        source=InMemorySource(DOCUMENTS),
    )


def test_a_durable_run_needs_a_state_directory_and_a_job_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli = make_cli(_pipeline())
    assert cli(["--output", str(tmp_path / "output"), "--job-id", "j1"]) == 0
    out = capsys.readouterr().out
    assert "status=completed" in out
    assert "restart_safe=True" in out


def test_omitting_a_storage_decision_is_an_error(tmp_path: Path) -> None:
    cli = make_cli(_pipeline())
    with pytest.raises(SystemExit):
        cli([])


def test_state_without_a_job_id_is_an_error(tmp_path: Path) -> None:
    cli = make_cli(_pipeline())
    with pytest.raises(SystemExit):
        cli(["--output", str(tmp_path / "output")])


def test_state_and_ephemeral_are_mutually_exclusive(tmp_path: Path) -> None:
    cli = make_cli(_pipeline())
    with pytest.raises(SystemExit):
        cli(
            [
                "--output",
                str(tmp_path / "output"),
                "--state",
                str(tmp_path / ".caravel"),
                "--ephemeral",
            ]
        )


def test_an_ephemeral_run_needs_no_job_id_and_says_so(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli = make_cli(_pipeline())
    assert cli(["--output", str(tmp_path / "output"), "--ephemeral"]) == 0
    assert "restart_safe=False" in capsys.readouterr().out


def test_params_reach_the_run(tmp_path: Path) -> None:
    seen: dict[str, str] = {}

    def capture_partition(partition, *, context: Any) -> Any:
        document: Any = partition.load()
        seen.update(context.params)
        return document

    def capture(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(capture_partition)

    pipeline = Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(capture)])],
        source=InMemorySource(DOCUMENTS),
    )
    cli = make_cli(pipeline)
    assert (
        cli(["--output", str(tmp_path / "output"), "--job-id", "j1", "--param", "env=staging"]) == 0
    )
    assert seen["env"] == "staging"


def test_a_malformed_param_is_rejected(tmp_path: Path) -> None:
    cli = make_cli(_pipeline())
    with pytest.raises(SystemExit):
        cli(["--output", str(tmp_path / "output"), "--job-id", "j1", "--param", "novalue"])


def test_the_graph_flag_prints_mermaid_without_running(
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = make_cli(_pipeline())
    assert cli(["--graph"]) == 0
    assert "flowchart" in capsys.readouterr().out.lower()


def test_resuming_the_same_job_reprocesses_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli = make_cli(_pipeline())
    cli(["--output", str(tmp_path / "output"), "--job-id", "j1"])
    capsys.readouterr()
    assert cli(["--output", str(tmp_path / "output"), "--job-id", "j1"]) == 0
    assert "batches=0" in capsys.readouterr().out


def test_help_describes_the_storage_decision(capsys: pytest.CaptureFixture[str]) -> None:
    cli = make_cli(_pipeline())
    with pytest.raises(SystemExit):
        cli(["--help"])
    help_text = capsys.readouterr().out
    for flag in ("--state", "--ephemeral", "--job-id", "--param", "--graph"):
        assert flag in help_text


def test_observers_passed_to_the_cli_reach_the_run(tmp_path: Path) -> None:
    events: list[str] = []

    class Recorder:
        plugin_id = "recorder"
        criticality = "best_effort"

        def on_event(self, event: Any) -> None:
            events.append(event.kind)

    cli = make_cli(_pipeline(), plugins=[Recorder()])
    assert cli(["--output", str(tmp_path / "output"), "--job-id", "j1"]) == 0
    assert "run_started" in events and "run_completed" in events


def test_committed_output_is_readable_after_a_cli_run(tmp_path: Path) -> None:
    from caravel import DurableStorage

    pipeline = _pipeline()
    cli = make_cli(pipeline)
    cli(["--output", str(tmp_path / "output"), "--job-id", "j1"])

    storage = DurableStorage.at(tmp_path / "output")
    try:
        results = storage.results(pipeline, job_id="j1").of("load")
        assert results["doc-1"] == {"id": "doc-1", "loaded": True}
        assert json.dumps(results, sort_keys=True)
    finally:
        storage.close()
