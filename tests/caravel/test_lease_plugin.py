"""Contract tests for the first-party LeasePlugin."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from caravel import JSONDataset, PartitionedJSONDataset, Pipeline, Stage, step
from caravel.plugins import (
    LeaseHeldError,
    LeaseIntegrityError,
    LeasePlugin,
    PluginFailureError,
    RunFacts,
    RunOutcome,
    UnsupportedLeaseVersionError,
    validate_lease,
)
from caravel.plugins import lease as lease_module
from caravel.runner import run


class _InjectedFailure(Exception):
    """Raised by armed failpoints to simulate an interrupted process."""


class _SeedLoader:
    def __init__(self, partitions: dict[str, dict[str, Any]]) -> None:
        self.name = "seed"
        self.partitions = partitions

    def load(self) -> dict[str, dict[str, Any]]:
        return {key: dict(record) for key, record in self.partitions.items()}


def _make_pipeline() -> Pipeline:
    @step(output=PartitionedJSONDataset(name="bronze_map"))
    def bronze_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        return {key: {**record, "mapped": True} for key, record in partitions.items()}

    @step(output=JSONDataset(name="silver_summary"))
    def silver_summary(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, int]:
        _ = context
        return {"count": len(partitions)}

    return Pipeline(
        name="lease_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[
            Stage(name="bronze", entries=[bronze_map]),
            Stage(name="silver", entries=[silver_summary]),
        ],
    )


def _plugin(tmp_path: Path, **kwargs: Any) -> LeasePlugin:
    return LeasePlugin(metadata_root=tmp_path / "lease_meta", **kwargs)


def _lease_file(tmp_path: Path) -> Path:
    return tmp_path / "lease_meta" / "v1" / "leases" / "lease_demo.json"


def _timestamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _foreign_lease(*, heartbeat_age_seconds: float) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    heartbeat = now - timedelta(seconds=heartbeat_age_seconds)
    return {
        "schema_version": 1,
        "pipeline": "lease_demo",
        "run_id": "0f" * 16,
        "holder": "other-host:4242",
        "host": "other-host",
        "pid": 4242,
        "started_at": _timestamp(heartbeat),
        "heartbeat_at": _timestamp(heartbeat),
    }


def _write_foreign_lease(tmp_path: Path, *, heartbeat_age_seconds: float) -> dict[str, Any]:
    lease = _foreign_lease(heartbeat_age_seconds=heartbeat_age_seconds)
    lease_file = _lease_file(tmp_path)
    lease_file.parent.mkdir(parents=True, exist_ok=True)
    lease_file.write_text(json.dumps(lease), "utf-8")
    return lease


def _run_facts() -> RunFacts:
    return RunFacts(
        pipeline_name="lease_demo",
        run_id=uuid.uuid4().hex,
        run_root="/runs",
        is_selective=False,
    )


def _arm_failpoint(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def hook(fired: str) -> None:
        if fired == name:
            raise _InjectedFailure(fired)

    monkeypatch.setattr(lease_module, "_failpoint_hook", hook)


# ---------------------------------------------------------------------------
# Plugin configuration and lease schema validation
# ---------------------------------------------------------------------------


def test_metadata_root_and_intervals_are_validated(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        LeasePlugin()  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="explicit metadata_root"):
        LeasePlugin(metadata_root="")
    with pytest.raises(ValueError, match="heartbeat_interval"):
        _plugin(tmp_path, heartbeat_interval=0)
    with pytest.raises(ValueError, match="stale_threshold"):
        _plugin(tmp_path, heartbeat_interval=10.0, stale_threshold=10.0)


def test_valid_lease_round_trips_validation() -> None:
    lease = _foreign_lease(heartbeat_age_seconds=0)
    assert validate_lease(lease, expected_pipeline="lease_demo") == lease


def test_unsupported_lease_version_raises() -> None:
    lease = _foreign_lease(heartbeat_age_seconds=0)
    lease["schema_version"] = 99
    with pytest.raises(UnsupportedLeaseVersionError, match="version 99"):
        validate_lease(lease, expected_pipeline="lease_demo")


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda r: r.update({"extra": True}), "unknown"),
        (lambda r: r.pop("holder"), "missing"),
        (lambda r: r.update({"pipeline": "other"}), "does not match"),
        (lambda r: r.update({"run_id": "nope"}), "run_id"),
        (lambda r: r.update({"holder": ""}), "holder"),
        (lambda r: r.update({"pid": 0}), "pid"),
        (lambda r: r.update({"heartbeat_at": "later"}), "heartbeat_at"),
    ],
)
def test_malformed_leases_are_rejected(mutate: Any, match: str) -> None:
    lease = _foreign_lease(heartbeat_age_seconds=0)
    mutate(lease)
    with pytest.raises(ValueError, match=match):
        validate_lease(lease, expected_pipeline="lease_demo")


# ---------------------------------------------------------------------------
# Acquisition, refusal, stale recovery, and release
# ---------------------------------------------------------------------------


def test_successful_run_acquires_heartbeats_and_releases(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    assert not _lease_file(tmp_path).exists()
    assert not plugin.heartbeat_thread_alive()


def test_apparent_live_lease_refuses_startup_before_user_code(tmp_path: Path) -> None:
    _write_foreign_lease(tmp_path, heartbeat_age_seconds=1.0)
    plugin = _plugin(tmp_path)

    with pytest.raises(PluginFailureError) as exc_info:
        run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])
    assert isinstance(exc_info.value.__cause__, LeaseHeldError)

    assert not (tmp_path / "runs" / "lease_demo").exists()
    lease = json.loads(_lease_file(tmp_path).read_text("utf-8"))
    assert lease["holder"] == "other-host:4242"


def test_stale_lease_is_recovered_with_evidence(tmp_path: Path) -> None:
    stale = _write_foreign_lease(tmp_path, heartbeat_age_seconds=300.0)
    plugin = _plugin(tmp_path, heartbeat_interval=1.0, stale_threshold=60.0)

    run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    recoveries_dir = tmp_path / "lease_meta" / "v1" / "recoveries" / "lease_demo"
    recovery_files = sorted(recoveries_dir.iterdir())
    assert len(recovery_files) == 1
    recovery = json.loads(recovery_files[0].read_text("utf-8"))
    assert recovery["abandoned_lease"] == stale
    assert recovery["abandoned_lease"]["run_id"] == stale["run_id"]
    assert not _lease_file(tmp_path).exists()


def test_externally_killed_process_leaves_classifiable_evidence(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path, heartbeat_interval=0.5, stale_threshold=1.0)
    facts = _run_facts()
    plugin.enter(facts)
    assert plugin.heartbeat_thread_alive()

    # Simulate a hard external kill: the process dies without exit(); the
    # daemon thread and lease file are simply abandoned.
    plugin._stop.set()
    plugin._thread.join(timeout=5.0)  # type: ignore[union-attr]

    survivor = json.loads(_lease_file(tmp_path).read_text("utf-8"))
    assert survivor["run_id"] == facts.run_id

    with pytest.raises(PluginFailureError):
        run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[_plugin(tmp_path)])

    time.sleep(1.1)
    recovering = _plugin(tmp_path, heartbeat_interval=0.4, stale_threshold=1.0)
    run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[recovering])
    assert not _lease_file(tmp_path).exists()


def test_heartbeat_refreshes_lease_evidence(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path, heartbeat_interval=0.05, stale_threshold=5.0)
    facts = _run_facts()

    def _read_heartbeat() -> str:
        # Lease writes are advisory and not atomic, so a concurrent reader can
        # observe a partially written file; retry until a full record lands.
        deadline = time.monotonic() + 5.0
        while True:
            try:
                return str(json.loads(_lease_file(tmp_path).read_text("utf-8"))["heartbeat_at"])
            except (json.JSONDecodeError, KeyError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

    plugin.enter(facts)
    try:
        first = _read_heartbeat()
        deadline = time.monotonic() + 5.0
        refreshed = first
        while refreshed == first and time.monotonic() < deadline:
            time.sleep(0.05)
            refreshed = _read_heartbeat()
        assert refreshed > first
    finally:
        plugin.exit(facts, RunOutcome(status="completed"))
    assert not plugin.heartbeat_thread_alive()


def test_malformed_lease_fails_closed_for_operator(tmp_path: Path) -> None:
    lease_file = _lease_file(tmp_path)
    lease_file.parent.mkdir(parents=True, exist_ok=True)
    lease_file.write_text("{not json", "utf-8")

    with pytest.raises(PluginFailureError) as exc_info:
        run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[_plugin(tmp_path)])
    assert isinstance(exc_info.value.__cause__, LeaseIntegrityError)
    assert lease_file.exists()


def test_unsupported_lease_version_fails_startup(tmp_path: Path) -> None:
    lease = _write_foreign_lease(tmp_path, heartbeat_age_seconds=0.0)
    lease["schema_version"] = 99
    _lease_file(tmp_path).write_text(json.dumps(lease), "utf-8")

    with pytest.raises(PluginFailureError) as exc_info:
        run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[_plugin(tmp_path)])
    assert isinstance(exc_info.value.__cause__, UnsupportedLeaseVersionError)


# ---------------------------------------------------------------------------
# Heartbeat and teardown failure semantics
# ---------------------------------------------------------------------------


def test_mid_run_heartbeat_failure_surfaces_at_teardown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin = _plugin(tmp_path, heartbeat_interval=0.05, stale_threshold=5.0)
    _arm_failpoint(monkeypatch, "before_heartbeat_write")

    facts = _run_facts()
    plugin.enter(facts)
    deadline = time.monotonic() + 5.0
    while plugin.heartbeat_thread_alive() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not plugin.heartbeat_thread_alive()

    with pytest.raises(LeaseIntegrityError, match="heartbeat"):
        plugin.exit(facts, RunOutcome(status="completed"))
    assert not _lease_file(tmp_path).exists()


def test_teardown_release_failure_fails_a_successful_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin = _plugin(tmp_path, heartbeat_interval=1.0, stale_threshold=5.0)
    _arm_failpoint(monkeypatch, "before_lease_release")

    with pytest.raises(PluginFailureError, match="teardown"):
        run(_make_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    assert not plugin.heartbeat_thread_alive()
    assert _lease_file(tmp_path).exists()


def test_lease_released_after_failed_run(tmp_path: Path) -> None:
    @step(output=PartitionedJSONDataset(name="boom"))
    def boom(partitions: dict[str, dict[str, Any]], *, context: object) -> Any:
        _ = context, partitions
        raise RuntimeError("boom")

    pipeline = Pipeline(
        name="lease_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[boom])],
    )
    plugin = _plugin(tmp_path)

    with pytest.raises(RuntimeError):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    assert not _lease_file(tmp_path).exists()
    assert not plugin.heartbeat_thread_alive()


def test_lease_contains_no_secrets(tmp_path: Path) -> None:
    canary = "canary-secret-value"
    plugin = LeasePlugin(
        metadata_root=tmp_path / "lease_meta",
        storage_options={"secret": canary},
        heartbeat_interval=1.0,
        stale_threshold=5.0,
    )
    facts = _run_facts()
    plugin.enter(facts)
    try:
        lease_text = _lease_file(tmp_path).read_text("utf-8")
        assert canary not in lease_text
        assert set(json.loads(lease_text)) == {
            "schema_version",
            "pipeline",
            "run_id",
            "holder",
            "host",
            "pid",
            "started_at",
            "heartbeat_at",
        }
    finally:
        plugin.exit(facts, RunOutcome(status="completed"))


# ---------------------------------------------------------------------------
# memory:// contract coverage
# ---------------------------------------------------------------------------


def test_lease_lifecycle_on_memory() -> None:
    base = f"memory://caravel_lease/{uuid.uuid4().hex}"
    plugin = LeasePlugin(metadata_root=f"{base}/meta", heartbeat_interval=1.0, stale_threshold=5.0)

    run(_make_pipeline(), run_root=f"{base}/runs", plugins=[plugin])

    import fsspec

    fs = fsspec.filesystem("memory")
    lease_path = f"{base.removeprefix('memory://')}/meta/v1/leases/lease_demo.json"
    assert not fs.exists(lease_path)
    assert not plugin.heartbeat_thread_alive()
