"""Plugin capabilities on the one durable execution model.

Only observers and run guards remain. Checkpoint, ownership, and lease
capabilities were retired when execution consolidated: the authoritative
progress store owns committed-output evidence, cross-run inventory, and worker
leases, so a plugin can no longer contradict it.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from caravel import DurabilityOptions, InMemorySource, Pipeline, Stage, Step, run
from caravel.plugins import (
    PluginFailureError,
    RunEvent,
    RunFacts,
    RunOutcome,
    validate_plugins,
)

DOCUMENTS = {"doc-1": {"id": "doc-1"}, "doc-2": {"id": "doc-2"}}


def load(value, *, context: Any) -> Any:
    documents: Any = dict(value)
    return documents


def summarize(value, *, context: Any) -> Any:
    return {"documents": len(value)}


def _pipeline() -> Pipeline:
    return Pipeline(
        name="documents",
        stages=[Stage(name="prepare", entries=[Step(load)])],
        durability_options=DurabilityOptions(schema_version="v1.0.0", job_id="j1"),
    )


class Recorder:
    """Best-effort observer that records the events it receives."""

    plugin_id = "recorder"
    criticality = "best_effort"

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def on_event(self, event: RunEvent) -> None:
        self.events.append(event)


class Exploder:
    plugin_id = "exploder"

    def __init__(self, criticality: str, *, on: str = "node_completed") -> None:
        self.criticality = criticality
        self._on = on

    def on_event(self, event: RunEvent) -> None:
        if event.kind == self._on:
            raise RuntimeError("observer exploded")


class Guard:
    def __init__(self, plugin_id: str, log: list[str], *, fail_exit: bool = False) -> None:
        self.plugin_id = plugin_id
        self._log = log
        self._fail_exit = fail_exit

    def enter(self, run: RunFacts) -> None:
        self._log.append(f"enter:{self.plugin_id}")

    def exit(self, run: RunFacts, outcome: RunOutcome) -> None:
        self._log.append(f"exit:{self.plugin_id}:{outcome.status}")
        if self._fail_exit:
            raise RuntimeError("teardown failed")


# ---------------------------------------------------------------------------
# Observers
# ---------------------------------------------------------------------------


def test_an_observer_sees_the_run_and_work_unit_lifecycle(tmp_path: Path) -> None:
    recorder = Recorder()
    run(
        _pipeline(),
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[recorder],
    )

    kinds = [event.kind for event in recorder.events]
    assert kinds[0] == "run_started"
    assert kinds[-1] == "run_completed"
    assert kinds.count("node_started") == 1
    assert kinds.count("node_completed") == 1


def test_event_facts_describe_the_durable_work_unit(tmp_path: Path) -> None:
    recorder = Recorder()
    run(
        _pipeline(),
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[recorder],
    )
    completed = [e for e in recorder.events if e.kind == "node_completed"]
    node = completed[0].node
    assert node is not None
    assert node.node_id == "stage-001-entry-001"
    assert node.dataset_type == "JSONDataset"
    assert node.name == "load"

    facts = completed[0].run
    assert facts.job_id == "j1"
    assert facts.pipeline_id == "documents"
    assert facts.restart_safe is True
    assert facts.invocation_id


def test_an_ephemeral_run_tells_observers_it_is_not_restart_safe(
    tmp_path: Path,
) -> None:
    recorder = Recorder()
    in_memory = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])
    run(
        in_memory,
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[recorder],
    )
    assert all(event.run.restart_safe is False for event in recorder.events)


@pytest.mark.parametrize("durable", [False, True])
def test_each_invocation_has_one_unique_id(tmp_path: Path, durable: bool) -> None:
    pipeline = _pipeline()
    if not durable:
        pipeline.durability_options = None
    invocation_ids = []
    for _ in range(2):
        recorder = Recorder()
        result = run(
            pipeline,
            source=InMemorySource(DOCUMENTS),
            output=tmp_path / "output",
            plugins=[recorder],
        )
        assert uuid.UUID(result.invocation_id).version == 4
        assert {event.run.invocation_id for event in recorder.events} == {result.invocation_id}
        assert result.job_id == ("j1" if durable else None)
        invocation_ids.append(result.invocation_id)
    assert invocation_ids[0] != invocation_ids[1]


def test_a_required_observer_failure_fails_the_run(tmp_path: Path) -> None:
    with pytest.raises(PluginFailureError, match="Required observer"):
        run(
            _pipeline(),
            source=InMemorySource(DOCUMENTS),
            output=tmp_path / "output",
            plugins=[Exploder("required")],
        )


def test_a_best_effort_observer_failure_does_not_fail_the_run(
    tmp_path: Path,
) -> None:
    run(
        _pipeline(),
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[Exploder("best_effort")],
    )


def test_committed_work_survives_a_required_observer_failure(tmp_path: Path) -> None:
    """An observer cannot undo what already committed."""
    pipeline = _pipeline()
    with pytest.raises(PluginFailureError):
        run(
            pipeline,
            source=InMemorySource(DOCUMENTS),
            output=tmp_path / "output",
            plugins=[Exploder("required")],
        )
    assert (tmp_path / "output").exists()


# ---------------------------------------------------------------------------
# Run guards
# ---------------------------------------------------------------------------


def test_guards_enter_in_order_and_exit_in_reverse(tmp_path: Path) -> None:
    log: list[str] = []
    run(
        _pipeline(),
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[Guard("outer", log), Guard("inner", log)],
    )
    assert log == [
        "enter:outer",
        "enter:inner",
        "exit:inner:completed",
        "exit:outer:completed",
    ]


def test_a_failing_run_still_exits_its_guards(tmp_path: Path) -> None:
    log: list[str] = []

    def explode(value, *, context: Any) -> Any:
        raise RuntimeError("step failed")

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(explode)])])
    with pytest.raises(RuntimeError, match="step failed"):
        run(
            pipeline,
            source=InMemorySource(DOCUMENTS),
            output=tmp_path / "output",
            plugins=[Guard("outer", log)],
        )
    assert log == ["enter:outer", "exit:outer:failed"]


def test_teardown_is_attempted_for_every_entered_guard(tmp_path: Path) -> None:
    log: list[str] = []
    with pytest.raises(PluginFailureError, match="teardown failed"):
        run(
            _pipeline(),
            source=InMemorySource(DOCUMENTS),
            output=tmp_path / "output",
            plugins=[Guard("outer", log), Guard("inner", log, fail_exit=True)],
        )
    # The outer guard still got its exit despite the inner one failing.
    assert "exit:outer:completed" in log


# ---------------------------------------------------------------------------
# Composition validation
# ---------------------------------------------------------------------------


def test_duplicate_plugin_ids_are_rejected() -> None:
    with pytest.raises(ValueError, match="Duplicate plugin id"):
        validate_plugins((Recorder(), Recorder()))


def test_a_plugin_with_no_capability_is_rejected() -> None:
    class Inert:
        plugin_id = "inert"

    with pytest.raises(ValueError, match="no supported capability"):
        validate_plugins((Inert(),))


def test_an_observer_must_declare_criticality_explicitly() -> None:
    class Vague:
        plugin_id = "vague"

        def on_event(self, event: RunEvent) -> None: ...

    with pytest.raises(ValueError, match="criticality"):
        validate_plugins((Vague(),))


def test_a_plugin_id_must_be_one_safe_path_segment() -> None:
    recorder = Recorder()
    recorder.plugin_id = "../escape"
    with pytest.raises(ValueError, match="safe path segment"):
        validate_plugins((recorder,))


def test_the_retired_capabilities_are_gone() -> None:
    import caravel.plugins as plugins

    for retired in ("CheckpointCapability", "OwnershipCapability", "CheckpointPlugin"):
        assert not hasattr(plugins, retired), f"{retired} should have been retired"


# ---------------------------------------------------------------------------
# Run evidence
# ---------------------------------------------------------------------------


def test_the_evidence_plugin_records_a_real_run(tmp_path: Path) -> None:
    """The shipped observer must accept the facts this engine actually emits.

    It validates its own payload against a field set, so a fact renamed in the
    engine turns every run into a plugin failure. That is only caught by
    running it, which is what this test does.
    """
    from caravel.plugins.run_evidence import RunEvidencePlugin, validate_run_event

    evidence = RunEvidencePlugin(metadata_root=tmp_path / "metadata")
    result = run(
        _pipeline(),
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[evidence],
    )

    events_dir = Path(evidence.events_dir(result.invocation_id))
    written = sorted(events_dir.glob("*.json"))
    assert written, "a completed run must leave evidence"

    kinds = []
    for path in written:
        payload = json.loads(path.read_text())
        # Re-validating proves the record on disk satisfies the same contract,
        # not merely that something was written.
        validate_run_event(payload)
        kinds.append(payload["kind"])

    assert kinds[0] == "run_started"
    assert kinds[-1] == "run_completed"
    assert kinds.count("node_started") == 1

    node_events = [
        json.loads(path.read_text()) for path in written if json.loads(path.read_text())["node"]
    ]
    node = node_events[0]["node"]
    assert node["node_id"] == "stage-001-entry-001"
    assert Path(evidence.summary_path(result.invocation_id)).is_file()


def test_the_evidence_plugin_records_every_declared_node(tmp_path: Path) -> None:
    from caravel.plugins.run_evidence import RunEvidencePlugin

    evidence = RunEvidencePlugin(metadata_root=tmp_path / "metadata")
    pipeline = Pipeline(
        name="documents",
        stages=[
            Stage(name="prepare", entries=[Step(load)]),
            Stage(name="finish", entries=[Step(summarize)]),
        ],
        durability_options=DurabilityOptions(schema_version="v1.0.0", job_id="j1"),
    )
    result = run(
        pipeline,
        source=InMemorySource(DOCUMENTS),
        output=tmp_path / "output",
        plugins=[evidence],
    )

    names = {
        json.loads(path.read_text())["node"]["name"]
        for path in Path(evidence.events_dir(result.invocation_id)).glob("*.json")
        if json.loads(path.read_text())["node"]
    }
    assert names == {"load", "summarize"}
