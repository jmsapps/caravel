"""Contract tests for the first-party RunEvidencePlugin."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    step,
)
from caravel.plan import compile_pipeline
from caravel.plugins import (
    CheckpointPlugin,
    PluginFailureError,
    RunEvidenceIntegrityError,
    RunEvidencePlugin,
    UnsupportedRunEventVersionError,
    validate_run_event,
)
from caravel.plugins import run_evidence as run_evidence_module
from caravel.runner import ExecutionRequest, RunResult, bind_execution, execute, run


class _InjectedFailure(Exception):
    """Raised by armed failpoints to simulate an interrupted process."""


class _SeedLoader:
    def __init__(self, partitions: dict[str, dict[str, Any]]) -> None:
        self.name = "seed"
        self.partitions = partitions

    def load(self) -> dict[str, dict[str, Any]]:
        return {key: dict(record) for key, record in self.partitions.items()}


def _make_pipeline(*, fail_in_silver: bool = False) -> Pipeline:
    @step(output=PartitionedJSONDataset(name="bronze_map"))
    def bronze_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        return {key: {**record, "mapped": True} for key, record in partitions.items()}

    @step(output=JSONDataset(name="silver_summary"))
    def silver_summary(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, int]:
        _ = context
        if fail_in_silver:
            raise RuntimeError("user step exploded with secret-arg")
        return {"count": len(partitions)}

    return Pipeline(
        name="evid_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[
            Stage(name="bronze", entries=[bronze_map]),
            Stage(name="silver", entries=[silver_summary]),
        ],
    )


def _plugin(tmp_path: Path, **kwargs: Any) -> RunEvidencePlugin:
    return RunEvidencePlugin(metadata_root=tmp_path / "evid_meta", **kwargs)


def _run_result(pipeline: Pipeline, tmp_path: Path, **kwargs: Any) -> RunResult:
    logical_plan = compile_pipeline(pipeline)
    request = ExecutionRequest(run_root=tmp_path / "runs", **kwargs)
    return execute(bind_execution(pipeline, logical_plan, request))


def _arm_failpoint(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def hook(fired: str) -> None:
        if fired == name:
            raise _InjectedFailure(fired)

    monkeypatch.setattr(run_evidence_module, "_failpoint_hook", hook)


def _disarm_failpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_evidence_module, "_failpoint_hook", None)


def _only_run_id(plugin: RunEvidencePlugin, tmp_path: Path) -> str:
    events_root = tmp_path / "evid_meta" / "v1" / "events"
    run_ids = sorted(p.name for p in events_root.iterdir())
    assert len(run_ids) == 1
    return run_ids[0]


# ---------------------------------------------------------------------------
# Plugin configuration and event schema validation
# ---------------------------------------------------------------------------


def test_metadata_root_and_criticality_are_validated(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        RunEvidencePlugin()  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="explicit metadata_root"):
        RunEvidencePlugin(metadata_root="")
    with pytest.raises(ValueError, match="criticality"):
        RunEvidencePlugin(metadata_root=tmp_path, criticality="sometimes")  # type: ignore[arg-type]
    assert _plugin(tmp_path).criticality == "required"


def _valid_event() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": "0f" * 16,
        "pipeline": "evid_demo",
        "is_selective": False,
        "run_root": "/runs",
        "sequence": 1,
        "kind": "run_started",
        "node": None,
        "error_type": None,
        "created_at": "2026-07-04T00:00:00.000000Z",
    }


def test_valid_event_round_trips_validation() -> None:
    event = _valid_event()
    assert validate_run_event(event) == event


def test_unsupported_event_version_raises() -> None:
    event = _valid_event()
    event["schema_version"] = 99
    with pytest.raises(UnsupportedRunEventVersionError, match="version 99"):
        validate_run_event(event)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda r: r.update({"extra": True}), "unknown"),
        (lambda r: r.pop("kind"), "missing"),
        (lambda r: r.update({"run_id": "nope"}), "run_id"),
        (lambda r: r.update({"sequence": 0}), "sequence"),
        (lambda r: r.update({"kind": "node_meditated"}), "not supported"),
        (lambda r: r.update({"node": {"node_id": "x"}}), "node fact fields"),
        (lambda r: r.update({"error_type": ""}), "error_type"),
        (lambda r: r.update({"created_at": "today"}), "created_at"),
    ],
)
def test_malformed_events_are_rejected(mutate: Any, match: str) -> None:
    event = _valid_event()
    mutate(event)
    with pytest.raises(ValueError, match=match):
        validate_run_event(event)


# ---------------------------------------------------------------------------
# Success, failure, and skip evidence
# ---------------------------------------------------------------------------


def test_successful_run_records_ordered_events_and_summary(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    run_id = _only_run_id(plugin, tmp_path)
    events = plugin.read_events(run_id)
    assert [event["kind"] for event in events] == [
        "run_started",
        "node_started",
        "node_completed",
        "node_started",
        "node_completed",
        "run_completed",
    ]
    assert [event["sequence"] for event in events] == [1, 2, 3, 4, 5, 6]

    summary = json.loads(Path(plugin.summary_path(run_id)).read_text("utf-8"))
    assert summary["status"] == "completed"
    assert summary["event_count"] == 6
    assert [node["outcome"] for node in summary["nodes"]] == ["completed", "completed"]
    assert summary["failure_types"] == []


def test_failed_run_records_failure_facts_without_message_text(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    with pytest.raises(RuntimeError):
        run(_make_pipeline(fail_in_silver=True), run_root=tmp_path / "runs", plugins=[plugin])

    run_id = _only_run_id(plugin, tmp_path)
    events = plugin.read_events(run_id)
    kinds = [event["kind"] for event in events]
    assert kinds[-2:] == ["node_failed", "run_failed"]
    failure_event = events[-1]
    assert failure_event["error_type"] == "RuntimeError"

    all_text = "".join(json.dumps(event) for event in events)
    assert "secret-arg" not in all_text

    summary = json.loads(Path(plugin.summary_path(run_id)).read_text("utf-8"))
    assert summary["status"] == "failed"
    assert summary["failure_types"] == ["RuntimeError"]


def test_skipped_nodes_reference_checkpoint_reuse(tmp_path: Path) -> None:
    checkpoint = CheckpointPlugin(metadata_root=tmp_path / "ckpt_meta")
    plugin = _plugin(tmp_path)
    pipeline = _make_pipeline()

    run(pipeline, run_root=tmp_path / "runs", plugins=[checkpoint, plugin])
    run(
        pipeline,
        run_root=tmp_path / "runs",
        only_stage="silver",
        plugins=[checkpoint, plugin],
    )

    events_root = tmp_path / "evid_meta" / "v1" / "events"
    run_ids = sorted(p.name for p in events_root.iterdir())
    assert len(run_ids) == 2
    selective_events = [plugin.read_events(run_id) for run_id in run_ids]
    skipped = [
        event for events in selective_events for event in events if event["kind"] == "node_skipped"
    ]
    assert len(skipped) == 1
    assert skipped[0]["node"]["node_id"] == "stage-001-entry-001"
    assert skipped[0]["is_selective"] is True


# ---------------------------------------------------------------------------
# Failure semantics
# ---------------------------------------------------------------------------


def test_required_evidence_failure_fails_run_but_keeps_committed_checkpoints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint = CheckpointPlugin(metadata_root=tmp_path / "ckpt_meta")
    plugin = _plugin(tmp_path, criticality="required")
    pipeline = _make_pipeline()

    run(pipeline, run_root=tmp_path / "runs", plugins=[checkpoint, plugin])
    committed = checkpoint.read_record("stage-001-entry-001")
    assert committed is not None

    _arm_failpoint(monkeypatch, "before_event_write")
    with pytest.raises(PluginFailureError):
        run(pipeline, run_root=tmp_path / "runs", plugins=[checkpoint, plugin])
    _disarm_failpoints(monkeypatch)

    assert checkpoint.read_record("stage-001-entry-001") == committed
    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[checkpoint, plugin])


def test_best_effort_evidence_failure_is_visible_but_not_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin = _plugin(tmp_path, criticality="best_effort")

    _arm_failpoint(monkeypatch, "before_event_write")
    result = _run_result(_make_pipeline(), tmp_path, plugins=(plugin,))
    _disarm_failpoints(monkeypatch)

    assert result.best_effort_errors
    assert all("run-evidence" in message for message in result.best_effort_errors)


def test_interrupted_summary_generation_leaves_events_regenerable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin = _plugin(tmp_path)

    _arm_failpoint(monkeypatch, "before_summary_write")
    with pytest.raises(PluginFailureError):
        run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    run_id = _only_run_id(plugin, tmp_path)
    assert not Path(plugin.summary_path(run_id)).exists()

    first = plugin.regenerate_summary(run_id)
    second = plugin.regenerate_summary(run_id)
    first.pop("generated_at")
    second.pop("generated_at")
    assert first == second
    assert first["status"] == "completed"


# ---------------------------------------------------------------------------
# Corrupt, missing, and unsupported evidence
# ---------------------------------------------------------------------------


def _event_files(tmp_path: Path, run_id: str) -> list[Path]:
    return sorted((tmp_path / "evid_meta" / "v1" / "events" / run_id).iterdir())


def test_corrupt_event_fails_summary_but_not_checkpoint_reuse(tmp_path: Path) -> None:
    checkpoint = CheckpointPlugin(metadata_root=tmp_path / "ckpt_meta")
    plugin = _plugin(tmp_path)
    pipeline = _make_pipeline()

    run(pipeline, run_root=tmp_path / "runs", plugins=[checkpoint, plugin])
    run_id = _only_run_id(plugin, tmp_path)
    _event_files(tmp_path, run_id)[0].write_text("{not json", "utf-8")

    with pytest.raises(RunEvidenceIntegrityError, match="unreadable or corrupt"):
        plugin.regenerate_summary(run_id)

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[checkpoint])


def test_missing_event_breaks_the_recorded_sequence(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    run_id = _only_run_id(plugin, tmp_path)
    _event_files(tmp_path, run_id)[2].unlink()

    with pytest.raises(RunEvidenceIntegrityError, match="incomplete or duplicated"):
        plugin.regenerate_summary(run_id)


def test_unsupported_event_version_raises_on_read(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    run_id = _only_run_id(plugin, tmp_path)
    event_file = _event_files(tmp_path, run_id)[0]
    event = json.loads(event_file.read_text("utf-8"))
    event["schema_version"] = 99
    event_file.write_text(json.dumps(event), "utf-8")

    with pytest.raises(UnsupportedRunEventVersionError):
        plugin.read_events(run_id)


# ---------------------------------------------------------------------------
# Sanitization
# ---------------------------------------------------------------------------


def test_events_and_summary_contain_no_secrets(tmp_path: Path) -> None:
    canary = "canary-secret-value"

    @step(output=PartitionedJSONDataset(name="bronze_map"))
    def bronze_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        return {key: {**record, "payload_canary": canary} for key, record in partitions.items()}

    pipeline = Pipeline(
        name="evid_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[bronze_map])],
    )
    plugin = RunEvidencePlugin(
        metadata_root=tmp_path / "evid_meta",
        storage_options={"secret": canary},
    )
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin], params={"token": canary})

    run_id = _only_run_id(plugin, tmp_path)
    for event_file in _event_files(tmp_path, run_id):
        assert canary not in event_file.read_text("utf-8")
    assert canary not in Path(plugin.summary_path(run_id)).read_text("utf-8")


# ---------------------------------------------------------------------------
# memory:// contract coverage
# ---------------------------------------------------------------------------


def test_events_and_summary_round_trip_on_memory() -> None:
    base = f"memory://caravel_evidence/{uuid.uuid4().hex}"
    plugin = RunEvidencePlugin(metadata_root=f"{base}/meta")

    run(_make_pipeline(), run_root=f"{base}/runs", plugins=[plugin])

    import fsspec

    fs = fsspec.filesystem("memory")
    events_root = f"{base.removeprefix('memory://')}/meta/v1/events"
    run_ids = sorted(str(path).rsplit("/", 1)[-1] for path in fs.ls(events_root, detail=False))
    assert len(run_ids) == 1
    events = plugin.read_events(run_ids[0])
    assert [event["kind"] for event in events][-1] == "run_completed"

    summary = plugin.regenerate_summary(run_ids[0])
    assert summary["status"] == "completed"
