"""Typed plugin capability tests.

Covers plugin validation, deterministic ordering and reverse teardown,
observer failure semantics, and the checkpoint capability seam driven
end-to-end by a recording fake.
"""

from pathlib import Path
from typing import Any

import pytest

from caravel.datasets import JSONDataset, PartitionedJSONDataset
from caravel.pipeline import Pipeline, Stage, step
from caravel.plugins import (
    CheckpointContext,
    CheckpointReuse,
    PluginFailureError,
    RunEvent,
    RunFacts,
    RunOutcome,
    validate_plugins,
)
from caravel.runner import ExecutionRequest, bind_execution, execute
from caravel.plan import compile_pipeline
from caravel.types import Dataset, MissingPriorOutputError, UnsupportedCapabilityError


class _StubLoader:
    name = "stub_loader"

    def __init__(self, partitions: dict[str, dict[str, object]]) -> None:
        self.partitions = partitions
        self.load_calls = 0

    def load(self) -> dict[str, dict[str, object]]:
        self.load_calls += 1
        return self.partitions


class RecordingObserver:
    def __init__(self, plugin_id: str, log: list[str], criticality: str = "best_effort") -> None:
        self.plugin_id = plugin_id
        self.criticality = criticality
        self.log = log
        self.events: list[RunEvent] = []

    def on_event(self, event: RunEvent) -> None:
        self.log.append(f"{self.plugin_id}:{event.kind}")
        self.events.append(event)


class FailingObserver:
    def __init__(self, plugin_id: str, criticality: str) -> None:
        self.plugin_id = plugin_id
        self.criticality = criticality

    def on_event(self, event: RunEvent) -> None:
        raise RuntimeError("observer exploded")


class RecordingGuard:
    def __init__(self, plugin_id: str, log: list[str]) -> None:
        self.plugin_id = plugin_id
        self.log = log
        self.outcomes: list[RunOutcome] = []

    def enter(self, run: RunFacts) -> None:
        self.log.append(f"{self.plugin_id}:enter")

    def exit(self, run: RunFacts, outcome: RunOutcome) -> None:
        self.log.append(f"{self.plugin_id}:exit:{outcome.status}")
        self.outcomes.append(outcome)


class FakeCheckpointPlugin:
    """Test-only checkpoint capability with in-memory evidence."""

    plugin_id = "fake-checkpoint"

    def __init__(self, grant: bool = True) -> None:
        self.grant = grant
        self.evidence: dict[str, str] = {}
        self.calls: list[str] = []
        self.contexts: list[CheckpointContext] = []

    def reuse_verdict(self, context: CheckpointContext, dataset: Dataset) -> bool:
        self.contexts.append(context)
        self.calls.append(f"verdict:{context.node.node_id}")
        return self.grant and context.node.node_id in self.evidence

    def before_replacement(self, context: CheckpointContext, dataset: Dataset) -> None:
        self.contexts.append(context)
        self.calls.append(f"invalidate:{context.node.node_id}")
        self.evidence.pop(context.node.node_id, None)

    def after_save(self, context: CheckpointContext, dataset: Dataset) -> None:
        self.contexts.append(context)
        self.calls.append(f"commit:{context.node.node_id}")
        assert context.node.step_dir is not None
        self.evidence[context.node.node_id] = context.node.step_dir


def test_committed_empty_reuse_must_be_reusable() -> None:
    with pytest.raises(ValueError, match="must be reusable"):
        CheckpointReuse(reusable=False, committed_empty=True)


def _linear_pipeline(loader: _StubLoader, calls: dict[str, int]) -> Pipeline:
    @step(output=PartitionedJSONDataset(name="bronze_out"))
    def bronze_map(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        calls["bronze"] = calls.get("bronze", 0) + 1
        return {key: {**record, "mapped": True} for key, record in partitions.items()}

    @step(output=JSONDataset(name="silver_out"))
    def silver_summary(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        calls["silver"] = calls.get("silver", 0) + 1
        return {"count": len(partitions)}

    return Pipeline(
        name="plugin_seam",
        loader=loader,
        stages=[
            Stage(name="bronze", entries=[bronze_map]),
            Stage(name="silver", entries=[silver_summary]),
        ],
    )


def _run(pipeline: Pipeline, tmp_path: Path, **kwargs: Any) -> Any:
    request = ExecutionRequest(run_root=tmp_path, **kwargs)
    plan = bind_execution(pipeline, compile_pipeline(pipeline), request)
    return execute(plan)


# ---------------------------------------------------------------- validation


def test_plugin_without_capability_is_rejected() -> None:
    class Inert:
        plugin_id = "inert"

    with pytest.raises(ValueError, match="declares no supported capability"):
        validate_plugins((Inert(),))


def test_duplicate_plugin_ids_are_rejected() -> None:
    log: list[str] = []
    with pytest.raises(ValueError, match="Duplicate plugin id"):
        validate_plugins((RecordingObserver("same", log), RecordingObserver("same", log)))


@pytest.mark.parametrize("bad_id", ["", "a/b", "a\\b", "..", None])
def test_unsafe_plugin_ids_are_rejected(bad_id: object) -> None:
    log: list[str] = []
    observer = RecordingObserver("placeholder", log)
    observer.plugin_id = bad_id  # type: ignore[assignment]
    with pytest.raises(ValueError):
        validate_plugins((observer,))


def test_observer_criticality_must_be_explicit() -> None:
    class NoCriticality:
        plugin_id = "silent"

        def on_event(self, event: RunEvent) -> None: ...

    with pytest.raises(ValueError, match="criticality"):
        validate_plugins((NoCriticality(),))


def test_only_one_checkpoint_capability_allowed() -> None:
    second = FakeCheckpointPlugin()
    second.plugin_id = "fake-checkpoint-2"  # type: ignore[misc]
    with pytest.raises(ValueError, match="Only one plugin may provide the checkpoint"):
        validate_plugins((FakeCheckpointPlugin(), second))


# ------------------------------------------------------- ordering / teardown


def test_observer_order_and_guard_reverse_teardown(tmp_path: Path) -> None:
    log: list[str] = []
    first_obs = RecordingObserver("first", log)
    second_obs = RecordingObserver("second", log)
    first_guard = RecordingGuard("guard-a", log)
    second_guard = RecordingGuard("guard-b", log)

    loader = _StubLoader({"a": {"id": "a"}})
    pipeline = _linear_pipeline(loader, {})

    result = _run(pipeline, tmp_path, plugins=(first_guard, first_obs, second_guard, second_obs))

    assert log[0] == "guard-a:enter"
    assert log[1] == "guard-b:enter"
    assert log[-2:] == ["guard-b:exit:completed", "guard-a:exit:completed"]

    event_pairs = [entry for entry in log if ":" in entry and "guard" not in entry]
    for i in range(0, len(event_pairs), 2):
        assert event_pairs[i].startswith("first:")
        assert event_pairs[i + 1].startswith("second:")
        assert event_pairs[i].split(":")[1] == event_pairs[i + 1].split(":")[1]

    kinds = [entry.split(":", 1)[1] for entry in event_pairs if entry.startswith("first:")]
    assert kinds == [
        "run_started",
        "node_started",
        "node_completed",
        "node_started",
        "node_completed",
        "run_completed",
    ]
    assert result.best_effort_errors == ()


def test_guards_receive_failed_outcome_and_run_failed_event(tmp_path: Path) -> None:
    log: list[str] = []
    observer = RecordingObserver("obs", log)
    guard = RecordingGuard("guard", log)

    @step(output=JSONDataset(name="boom"))
    def explode(partitions: object, *, context: object) -> object:
        _ = context
        raise RuntimeError("user step failed")

    pipeline = Pipeline(
        name="failing_run",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[explode])],
    )

    with pytest.raises(RuntimeError, match="user step failed"):
        _run(pipeline, tmp_path, plugins=(guard, observer))

    assert "guard:exit:failed" in log
    kinds = [event.kind for event in observer.events]
    assert kinds == ["run_started", "node_started", "node_failed", "run_failed"]
    failed_event = observer.events[-2]
    assert failed_event.error_type == "RuntimeError"
    assert "user step failed" not in repr(failed_event)


def test_required_observer_failure_fails_the_run(tmp_path: Path) -> None:
    loader = _StubLoader({"a": {"id": "a"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)

    with pytest.raises(PluginFailureError, match="Required observer 'boom'"):
        _run(pipeline, tmp_path, plugins=(FailingObserver("boom", "required"),))


def test_best_effort_observer_failure_is_collected_not_fatal(tmp_path: Path) -> None:
    loader = _StubLoader({"a": {"id": "a"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)

    result = _run(pipeline, tmp_path, plugins=(FailingObserver("soft", "best_effort"),))

    assert calls == {"bronze": 1, "silver": 1}
    assert result.best_effort_errors
    assert all("soft" in message for message in result.best_effort_errors)


def test_guard_startup_failure_prevents_user_code(tmp_path: Path) -> None:
    class BrokenGuard:
        plugin_id = "broken-guard"

        def enter(self, run: RunFacts) -> None:
            raise RuntimeError("cannot acquire")

        def exit(self, run: RunFacts, outcome: RunOutcome) -> None: ...

    loader = _StubLoader({"a": {"id": "a"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)

    with pytest.raises(PluginFailureError, match="failed to start"):
        _run(pipeline, tmp_path, plugins=(BrokenGuard(),))

    assert calls == {}
    assert loader.load_calls == 0


def test_guard_teardown_failure_fails_a_successful_run(tmp_path: Path) -> None:
    class LeakyGuard:
        plugin_id = "leaky-guard"

        def enter(self, run: RunFacts) -> None: ...

        def exit(self, run: RunFacts, outcome: RunOutcome) -> None:
            raise RuntimeError("release failed")

    loader = _StubLoader({"a": {"id": "a"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)

    with pytest.raises(PluginFailureError, match="teardown failed"):
        _run(pipeline, tmp_path, plugins=(LeakyGuard(),))

    assert calls == {"bronze": 1, "silver": 1}


# ------------------------------------------------------ checkpoint seam


def test_checkpoint_capability_enables_selective_execution(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _StubLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)
    plugin = FakeCheckpointPlugin()

    run(pipeline, run_root=tmp_path, plugins=[plugin])
    assert calls == {"bronze": 1, "silver": 1}
    assert "invalidate:stage-001-entry-001" in plugin.calls
    assert plugin.calls.index("invalidate:stage-001-entry-001") < plugin.calls.index(
        "commit:stage-001-entry-001"
    )

    run(pipeline, run_root=tmp_path, only_stage="silver", plugins=[plugin])

    assert calls == {"bronze": 1, "silver": 2}
    assert "verdict:stage-001-entry-001" in plugin.calls
    verdict_context = next(
        context
        for context in plugin.contexts
        if context.node.node_id == "stage-001-entry-001" and context.run.is_selective
    )
    assert verdict_context.run.pipeline_name == pipeline.name
    assert verdict_context.run.run_root == str(tmp_path)
    assert len(verdict_context.run.run_id) == 32

    silver_file = (
        tmp_path
        / pipeline.name
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    )
    assert silver_file.exists()


def test_checkpoint_capability_denial_forces_recompute_error(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _StubLoader({"a": {"id": "a"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)
    plugin = FakeCheckpointPlugin(grant=False)

    run(pipeline, run_root=tmp_path, plugins=[plugin])

    with pytest.raises(MissingPriorOutputError, match="no committed evidence"):
        run(pipeline, run_root=tmp_path, only_stage="silver", plugins=[plugin])

    assert calls == {"bronze": 1, "silver": 1}


def test_selective_still_fails_closed_without_capability_plugin(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _StubLoader({"a": {"id": "a"}})
    calls: dict[str, int] = {}
    pipeline = _linear_pipeline(loader, calls)
    log: list[str] = []

    run(pipeline, run_root=tmp_path, plugins=[RecordingObserver("only-obs", log)])

    with pytest.raises(UnsupportedCapabilityError):
        run(
            pipeline,
            run_root=tmp_path,
            only_stage="silver",
            plugins=[RecordingObserver("only-obs", log)],
        )


def test_skipped_node_emits_node_skipped_event(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _StubLoader({"a": {"id": "a"}})
    pipeline = _linear_pipeline(loader, {})
    plugin = FakeCheckpointPlugin()
    log: list[str] = []
    observer = RecordingObserver("obs", log)

    run(pipeline, run_root=tmp_path, plugins=[plugin, observer])
    observer.events.clear()

    run(pipeline, run_root=tmp_path, only_stage="silver", plugins=[plugin, observer])

    kinds = [event.kind for event in observer.events]
    assert kinds == [
        "run_started",
        "node_skipped",
        "node_started",
        "node_completed",
        "run_completed",
    ]
    skipped = observer.events[1]
    assert skipped.node is not None
    assert skipped.node.node_id == "stage-001-entry-001"


def test_plugins_empty_executes_without_plugin_callbacks(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _StubLoader({"a": {"id": "a"}})
    pipeline = _linear_pipeline(loader, {})

    run(pipeline, run_root=tmp_path, plugins=[])
