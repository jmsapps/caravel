"""Plugin capabilities on the one durable execution model.

Only observers and run guards remain. Checkpoint, ownership, and lease
capabilities were retired when execution consolidated: the authoritative
progress store owns committed-output evidence, cross-run inventory, and worker
leases, so a plugin can no longer contradict it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from caravel import DurableStorage, InMemorySource, Pipeline, Stage, Step, run
from caravel.plugins import (
    PluginFailureError,
    RunEvent,
    RunFacts,
    RunOutcome,
    validate_plugins,
)

DOCUMENTS = {"doc-1": {"id": "doc-1"}, "doc-2": {"id": "doc-2"}}


def load_partition(partition, *, context: Any) -> Any:
    document: Any = partition.load()
    return document


def load(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(load_partition)


def _pipeline() -> Pipeline:
    return Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(load)])])


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
    result = run(
        _pipeline(),
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
        plugins=[recorder],
    )
    assert result.status == "completed"

    kinds = [event.kind for event in recorder.events]
    assert kinds[0] == "run_started"
    assert kinds[-1] == "run_completed"
    assert kinds.count("node_started") == len(DOCUMENTS)
    assert kinds.count("node_completed") == len(DOCUMENTS)


def test_event_facts_describe_the_durable_work_unit(tmp_path: Path) -> None:
    recorder = Recorder()
    run(
        _pipeline(),
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
        plugins=[recorder],
    )
    completed = [e for e in recorder.events if e.kind == "node_completed"]
    node = completed[0].node
    assert node is not None
    assert node.partition_id in DOCUMENTS
    assert node.input_scope == "partition"
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
    run(
        _pipeline(),
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.ephemeral(tmp_path / "output"),
        plugins=[recorder],
    )
    assert all(event.run.restart_safe is False for event in recorder.events)


def test_a_required_observer_failure_fails_the_run(tmp_path: Path) -> None:
    with pytest.raises(PluginFailureError, match="Required observer"):
        run(
            _pipeline(),
            job_id="j1",
            source=InMemorySource(DOCUMENTS),
            storage=DurableStorage.at(tmp_path / "output"),
            plugins=[Exploder("required")],
        )


def test_a_best_effort_observer_failure_does_not_fail_the_run(
    tmp_path: Path,
) -> None:
    result = run(
        _pipeline(),
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
        plugins=[Exploder("best_effort")],
    )
    assert result.status == "completed"


def test_committed_work_survives_a_required_observer_failure(tmp_path: Path) -> None:
    """An observer cannot undo what already committed."""
    storage = DurableStorage.at(tmp_path / "output")
    pipeline = _pipeline()
    with pytest.raises(PluginFailureError):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource(DOCUMENTS),
            storage=storage,
            plugins=[Exploder("required")],
        )
    assert len(storage.results(pipeline, job_id="j1").of("load")) >= 1


# ---------------------------------------------------------------------------
# Run guards
# ---------------------------------------------------------------------------


def test_guards_enter_in_order_and_exit_in_reverse(tmp_path: Path) -> None:
    log: list[str] = []
    run(
        _pipeline(),
        job_id="j1",
        source=InMemorySource(DOCUMENTS),
        storage=DurableStorage.at(tmp_path / "output"),
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

    def explode_partition(partition, *, context: Any) -> Any:
        raise RuntimeError("step failed")

    def explode(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(explode_partition)

    pipeline = Pipeline(name="documents", stages=[Stage(name="prepare", entries=[Step(explode)])])
    with pytest.raises(RuntimeError, match="step failed"):
        run(
            pipeline,
            job_id="j1",
            source=InMemorySource(DOCUMENTS),
            storage=DurableStorage.at(tmp_path / "output"),
            plugins=[Guard("outer", log)],
        )
    assert log == ["enter:outer", "exit:outer:failed"]


def test_teardown_is_attempted_for_every_entered_guard(tmp_path: Path) -> None:
    log: list[str] = []
    with pytest.raises(PluginFailureError, match="teardown failed"):
        run(
            _pipeline(),
            job_id="j1",
            source=InMemorySource(DOCUMENTS),
            storage=DurableStorage.at(tmp_path / "output"),
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
