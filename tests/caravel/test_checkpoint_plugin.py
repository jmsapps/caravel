"""Contract tests for the first-party CheckpointPlugin."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    CheckpointCommitUncertainError,
    CheckpointIntegrityError,
    JSONDataset,
    MissingPriorOutputError,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    UnsupportedCapabilityError,
    UnsupportedCheckpointVersionError,
    step,
)
from caravel import runner as runner_module
from caravel.plugins import CheckpointPlugin, validate_checkpoint_record
from caravel.plugins import checkpoint as checkpoint_module
from caravel.runner import run

BRONZE_NODE_ID = "stage-001-entry-001"
SILVER_NODE_ID = "stage-002-entry-001"


class _InjectedFailure(Exception):
    """Raised by armed failpoints to simulate an interrupted process."""


class _SeedLoader:
    """Loader whose payload can be mutated between runs."""

    def __init__(self, partitions: dict[str, dict[str, Any]]) -> None:
        self.name = "seed"
        self.partitions = partitions

    def load(self) -> dict[str, dict[str, Any]]:
        return {key: dict(record) for key, record in self.partitions.items()}


def _make_pipeline(
    loader: _SeedLoader,
    calls: dict[str, int] | None = None,
    *,
    bronze_allow_empty: bool = False,
    bronze_stage_root: Path | str | None = None,
) -> Pipeline:
    counters = calls if calls is not None else {}

    @step(output=PartitionedJSONDataset(name="bronze_map", allow_empty=bronze_allow_empty))
    def bronze_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        counters["bronze_map"] = counters.get("bronze_map", 0) + 1
        return {key: {**record, "mapped": True} for key, record in partitions.items()}

    @step(output=JSONDataset(name="silver_summary"))
    def silver_summary(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, int]:
        _ = context
        counters["silver_summary"] = counters.get("silver_summary", 0) + 1
        return {"count": len(partitions)}

    return Pipeline(
        name="ckpt_demo",
        loader=loader,
        stages=[
            Stage(name="bronze", entries=[bronze_map], stage_root=bronze_stage_root),
            Stage(name="silver", entries=[silver_summary]),
        ],
    )


def _plugin(tmp_path: Path) -> CheckpointPlugin:
    return CheckpointPlugin(metadata_root=tmp_path / "ckpt_meta")


def _arm_failpoint(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def hook(fired: str) -> None:
        if fired == name:
            raise _InjectedFailure(fired)

    monkeypatch.setattr(checkpoint_module, "_failpoint_hook", hook)


def _disarm_failpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(checkpoint_module, "_failpoint_hook", None)


def _pipeline_root(run_root: Path) -> Path:
    return run_root / "ckpt_demo"


def _silver_payload(run_root: Path) -> dict[str, Any]:
    silver_file = (
        _pipeline_root(run_root)
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    )
    return json.loads(silver_file.read_text("utf-8"))


# ---------------------------------------------------------------------------
# Plugin configuration
# ---------------------------------------------------------------------------


def test_metadata_root_is_required_and_explicit() -> None:
    with pytest.raises(TypeError):
        CheckpointPlugin()  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="explicit metadata_root"):
        CheckpointPlugin(metadata_root="")
    with pytest.raises(ValueError, match="explicit metadata_root"):
        CheckpointPlugin(metadata_root="   ")


def test_record_path_lives_beneath_versioned_plugin_root(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    expected = tmp_path / "ckpt_meta" / "v1" / "checkpoints" / f"{BRONZE_NODE_ID}.json"
    assert Path(plugin.record_path(BRONZE_NODE_ID)) == expected


# ---------------------------------------------------------------------------
# Record schema validation
# ---------------------------------------------------------------------------


def _valid_record(node_id: str = BRONZE_NODE_ID) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": "0f" * 16,
        "node_id": node_id,
        "pipeline": "ckpt_demo",
        "stage": {"index": 1, "name": "bronze"},
        "step": {"index": 1, "name": "bronze_map"},
        "dataset": {"type": "PartitionedJSONDataset", "name": "bronze_map"},
        "output_path": "/runs/ckpt_demo/_001_bronze/_001_bronze_map",
        "partition_keys": ["a", "b"],
        "count": 2,
        "created_at": "2026-07-03T00:00:00.000000Z",
    }


def test_valid_record_round_trips_validation() -> None:
    record = _valid_record()
    assert validate_checkpoint_record(record, expected_node_id=BRONZE_NODE_ID) == record


def test_unsupported_schema_version_raises() -> None:
    record = _valid_record()
    record["schema_version"] = 2
    with pytest.raises(UnsupportedCheckpointVersionError, match="version 2"):
        validate_checkpoint_record(record, expected_node_id=BRONZE_NODE_ID)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda r: r.update({"schema_version": "1"}), "schema_version"),
        (lambda r: r.update({"extra": True}), "unknown"),
        (lambda r: r.pop("count"), "missing"),
        (lambda r: r.update({"run_id": "UPPER" * 7}), "run_id"),
        (lambda r: r.update({"node_id": "stage-1-entry-1"}), "node_id"),
        (lambda r: r.update({"pipeline": ""}), "pipeline"),
        (lambda r: r.update({"stage": {"index": 0, "name": "bronze"}}), "stage"),
        (lambda r: r.update({"dataset": {"type": "CustomDataset", "name": "x"}}), "not supported"),
        (lambda r: r.update({"partition_keys": ["b", "a"]}), "sorted"),
        (lambda r: r.update({"partition_keys": ["a", "a"]}), "sorted"),
        (lambda r: r.update({"count": 3}), "count"),
        (lambda r: r.update({"partition_keys": None}), "count"),
        (lambda r: r.update({"created_at": "2026-07-03 00:00:00"}), "created_at"),
    ],
)
def test_malformed_records_are_rejected(mutate: Any, match: str) -> None:
    record = _valid_record()
    mutate(record)
    with pytest.raises(ValueError, match=match):
        validate_checkpoint_record(record, expected_node_id=BRONZE_NODE_ID)


def test_record_node_id_must_match_expected() -> None:
    record = _valid_record()
    with pytest.raises(ValueError, match="does not match expected"):
        validate_checkpoint_record(record, expected_node_id=SILVER_NODE_ID)


# ---------------------------------------------------------------------------
# Commit protocol: layout, provenance, and sanitization
# ---------------------------------------------------------------------------


def test_step_directories_contain_dataset_files_only(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}}))
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    bronze_dir = _pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map"
    silver_dir = _pipeline_root(tmp_path / "runs") / "_002_silver" / "_001_silver_summary"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]
    assert sorted(p.name for p in silver_dir.iterdir()) == ["_001_silver_summary.json"]

    checkpoints_dir = tmp_path / "ckpt_meta" / "v1" / "checkpoints"
    assert sorted(p.name for p in checkpoints_dir.iterdir()) == [
        f"{BRONZE_NODE_ID}.json",
        f"{SILVER_NODE_ID}.json",
    ]


def test_committed_records_share_one_run_id_and_validate(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    bronze = plugin.read_record(BRONZE_NODE_ID)
    silver = plugin.read_record(SILVER_NODE_ID)
    assert bronze is not None and silver is not None
    assert bronze["run_id"] == silver["run_id"]
    assert bronze["partition_keys"] == ["a"]
    assert bronze["count"] == 1
    assert silver["partition_keys"] is None
    assert silver["count"] == 1
    assert bronze["output_path"].endswith("_001_bronze/_001_bronze_map")


def test_rerun_replaces_records_with_new_run_id(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    first = plugin.read_record(BRONZE_NODE_ID)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    second = plugin.read_record(BRONZE_NODE_ID)

    assert first is not None and second is not None
    assert first["run_id"] != second["run_id"]


def test_records_contain_no_secrets_or_payload_values(tmp_path: Path) -> None:
    canary = "canary-secret-value"

    @step(output=PartitionedJSONDataset(name="bronze_map"))
    def bronze_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        return {key: {**record, "payload_canary": canary} for key, record in partitions.items()}

    pipeline = Pipeline(
        name="ckpt_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[bronze_map])],
    )
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin], params={"token": canary})

    record_file = tmp_path / "ckpt_meta" / "v1" / "checkpoints" / f"{BRONZE_NODE_ID}.json"
    record_text = record_file.read_text("utf-8")
    assert canary not in record_text
    record = json.loads(record_text)
    assert set(record) == {
        "schema_version",
        "run_id",
        "node_id",
        "pipeline",
        "stage",
        "step",
        "dataset",
        "output_path",
        "partition_keys",
        "count",
        "created_at",
    }


def test_metadata_root_survives_clean_dirs(tmp_path: Path) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    pipeline.stages[0].clean_dirs = True
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    assert (tmp_path / "ckpt_meta" / "v1" / "checkpoints").exists()
    assert plugin.read_record(BRONZE_NODE_ID) is not None


def test_fewer_key_replacement_leaves_no_stale_partitions(tmp_path: Path) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    loader.partitions = {"a": {"id": "a"}}
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    bronze_dir = _pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json"]

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert _silver_payload(tmp_path / "runs") == {"count": 1}


def test_empty_partitioned_checkpoint_round_trips_without_sentinel(tmp_path: Path) -> None:
    loader = _SeedLoader({})
    pipeline = _make_pipeline(loader, bronze_allow_empty=True)
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    record = plugin.read_record(BRONZE_NODE_ID)
    assert record is not None
    assert record["partition_keys"] == []
    assert record["count"] == 0
    bronze_dir = _pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map"
    assert list(bronze_dir.iterdir()) == []

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert _silver_payload(tmp_path / "runs") == {"count": 0}


def test_stage_root_output_is_checked_from_plugin_metadata(tmp_path: Path) -> None:
    external_root = tmp_path / "external_bronze"
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader, bronze_stage_root=external_root)
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    record = plugin.read_record(BRONZE_NODE_ID)
    assert record is not None
    assert record["output_path"] == str(external_root / "_001_bronze_map")

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert _silver_payload(tmp_path / "runs") == {"count": 1}


def test_selective_step_reuses_committed_prior_step(tmp_path: Path) -> None:
    calls: dict[str, int] = {}

    @step(output=PartitionedJSONDataset(name="first_map"))
    def first_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        calls["first_map"] = calls.get("first_map", 0) + 1
        return partitions

    @step(output=PartitionedJSONDataset(name="second_map"))
    def second_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        calls["second_map"] = calls.get("second_map", 0) + 1
        return {key: {**record, "second": True} for key, record in partitions.items()}

    pipeline = Pipeline(
        name="ckpt_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[first_map, second_map])],
    )
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    run(
        pipeline,
        run_root=tmp_path / "runs",
        only_stage="bronze",
        only_step="second_map",
        plugins=[plugin],
    )

    assert calls == {"first_map": 1, "second_map": 2}


def test_route_step_records_encode_route_step_index(tmp_path: Path) -> None:
    from caravel import Branch

    @step(output=PartitionedJSONDataset(name="parsed"), persist=False)
    def parse(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    @step(output=PartitionedJSONDataset(name="normalized_json", allow_empty=True))
    def normalize(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    @step(output=PartitionedJSONDataset(name="decoded", allow_empty=True))
    def decode(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(
        name="by_source",
        by="source",
        routes={"json": [parse, normalize], "text": [decode]},
    )
    pipeline = Pipeline(
        name="ckpt_demo",
        loader=_SeedLoader({"x": {"__source__": "json"}, "y": {"__source__": "text"}}),
        stages=[Stage(name="bronze", entries=[branch])],
    )
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    normalize_record = plugin.read_record("stage-001-entry-001-route-001-step-002")
    decode_record = plugin.read_record("stage-001-entry-001-route-002-step-001")
    assert normalize_record is not None and decode_record is not None
    assert normalize_record["step"] == {"index": 2, "name": "normalize"}
    assert decode_record["step"] == {"index": 1, "name": "decode"}
    assert plugin.read_record("stage-001-entry-001-route-001-step-001") is None


def test_output_without_record_is_not_a_checkpoint(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)

    bronze_dir = _pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map"
    PartitionedJSONDataset(name="bronze_map").save({"a": {"id": "a", "mapped": True}}, bronze_dir)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_removing_the_plugin_disables_checkpoint_dependent_selection(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    with pytest.raises(UnsupportedCapabilityError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[])


def test_custom_dataset_cannot_be_a_selective_boundary(tmp_path: Path) -> None:
    class CustomDataset:
        def __init__(self) -> None:
            self.name = "custom"

        def load(self) -> Any:
            raise NotImplementedError

        def load_from(self, dest: Path | str) -> Any:
            _ = dest
            return {"a": {"id": "a"}}

        def save(self, payload: Any, dest: Path | str) -> None:
            Path(dest).mkdir(parents=True, exist_ok=True)
            (Path(dest) / "custom.json").write_text(json.dumps(payload), "utf-8")

        def exists(self, dest: Path | str) -> bool:
            _ = dest
            return True

        def describe(self) -> dict[str, Any]:
            return {"dataset": "CustomDataset", "name": self.name}

    @step(output=CustomDataset())
    def bronze_map(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        return partitions

    @step(output=JSONDataset(name="silver_summary"))
    def silver_summary(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, int]:
        _ = context
        return {"count": len(partitions)}

    pipeline = Pipeline(
        name="ckpt_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[
            Stage(name="bronze", entries=[bronze_map]),
            Stage(name="silver", entries=[silver_summary]),
        ],
    )
    plugin = _plugin(tmp_path)

    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    assert plugin.read_record(BRONZE_NODE_ID) is None

    with pytest.raises(UnsupportedCapabilityError, match="checkpoint inspection"):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


# ---------------------------------------------------------------------------
# Interrupted replacement via deterministic failpoints
# ---------------------------------------------------------------------------


def test_failure_before_invalidation_preserves_prior_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    loader.partitions = {"a": {"id": "a"}}
    _arm_failpoint(monkeypatch, "before_record_invalidation")
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert _silver_payload(tmp_path / "runs") == {"count": 2}


@pytest.mark.parametrize(
    "failpoint_name",
    [
        "during_record_invalidation",
        "after_record_invalidation",
        "before_record_write",
    ],
)
def test_plugin_failpoints_after_invalidation_leave_node_uncommitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failpoint_name: str
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    _arm_failpoint(monkeypatch, failpoint_name)
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_failure_after_output_cleanup_leaves_node_uncommitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    original = runner_module.remove_and_recreate_dir

    def cleanup_then_fail(path: Any, storage_options: Any = None) -> None:
        original(path, storage_options)
        raise _InjectedFailure("after_output_cleanup")

    monkeypatch.setattr(runner_module, "remove_and_recreate_dir", cleanup_then_fail)
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    monkeypatch.setattr(runner_module, "remove_and_recreate_dir", original)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_failure_after_partial_partition_write_leaves_node_uncommitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    original_save = PartitionedJSONDataset.save

    def partial_save(self: PartitionedJSONDataset, payload: Any, dest: Path | str) -> None:
        first_key = sorted(payload)[0]
        original_save(self, {first_key: payload[first_key]}, dest)
        raise _InjectedFailure("after_output_file_written:1")

    monkeypatch.setattr(PartitionedJSONDataset, "save", partial_save)
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    monkeypatch.setattr(PartitionedJSONDataset, "save", original_save)

    bronze_dir = _pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json"]
    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_failure_during_record_write_commits_via_rediscovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exception after the record bytes land is resolved by rediscovery."""
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)

    _arm_failpoint(monkeypatch, "after_record_write")
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    assert plugin.read_record(BRONZE_NODE_ID) is not None
    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert _silver_payload(tmp_path / "runs") == {"count": 1}


def test_unreadable_publication_outcome_raises_commit_uncertain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)

    def broken_read(node_id: str) -> dict[str, Any] | None:
        raise OSError(f"read-back unavailable for {node_id}")

    _arm_failpoint(monkeypatch, "after_record_write")
    monkeypatch.setattr(plugin, "read_record", broken_read)
    with pytest.raises(CheckpointCommitUncertainError):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])


def test_readback_mismatch_raises_commit_uncertain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)

    original_read = plugin.read_record

    def mismatched_read(node_id: str) -> dict[str, Any] | None:
        record = original_read(node_id)
        if record is not None:
            record["run_id"] = "0f" * 16
        return record

    monkeypatch.setattr(plugin, "read_record", mismatched_read)
    with pytest.raises(CheckpointCommitUncertainError, match="read-back"):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])


def test_failure_after_record_verified_still_committed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    plugin = _plugin(tmp_path)

    _arm_failpoint(monkeypatch, "after_record_verified")
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert _silver_payload(tmp_path / "runs") == {"count": 1}


# ---------------------------------------------------------------------------
# Selective-boundary verification against tampered or inconsistent state
# ---------------------------------------------------------------------------


def _bronze_record_file(tmp_path: Path) -> Path:
    return tmp_path / "ckpt_meta" / "v1" / "checkpoints" / f"{BRONZE_NODE_ID}.json"


def test_malformed_record_means_no_checkpoint(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    _bronze_record_file(tmp_path).write_text("{not json", encoding="utf-8")

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_unsupported_record_version_raises_at_boundary(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    record_file = _bronze_record_file(tmp_path)
    record = json.loads(record_file.read_text("utf-8"))
    record["schema_version"] = 99
    record_file.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(UnsupportedCheckpointVersionError):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_record_declaration_mismatch_raises_integrity_error(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    record_file = _bronze_record_file(tmp_path)
    record = json.loads(record_file.read_text("utf-8"))
    record["dataset"] = {"type": "PartitionedTextDataset", "name": "bronze_map"}
    record_file.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(CheckpointIntegrityError, match="does not match"):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_missing_physical_partition_raises_integrity_error(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}}))
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    (_pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map" / "b.json").unlink()

    with pytest.raises(CheckpointIntegrityError, match="missing=\\['b'\\]"):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_extra_physical_partition_raises_integrity_error(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    plugin = _plugin(tmp_path)
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    stray = _pipeline_root(tmp_path / "runs") / "_001_bronze" / "_001_bronze_map" / "z.json"
    stray.write_text("{}", encoding="utf-8")

    with pytest.raises(CheckpointIntegrityError, match="extra=\\['z'\\]"):
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])


def test_integrity_errors_do_not_leak_storage_options(tmp_path: Path) -> None:
    canary = "canary-secret-value"
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    plugin = CheckpointPlugin(
        metadata_root=tmp_path / "ckpt_meta",
        storage_options={"secret": canary},
    )
    run(pipeline, run_root=tmp_path / "runs", plugins=[plugin])

    record_file = _bronze_record_file(tmp_path)
    record = json.loads(record_file.read_text("utf-8"))
    record["dataset"] = {"type": "PartitionedTextDataset", "name": "bronze_map"}
    record_file.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(CheckpointIntegrityError) as exc_info:
        run(pipeline, run_root=tmp_path / "runs", only_stage="silver", plugins=[plugin])
    assert canary not in str(exc_info.value)
    assert canary not in record_file.read_text("utf-8")


# ---------------------------------------------------------------------------
# memory:// contract coverage
# ---------------------------------------------------------------------------


def _memory_root(label: str) -> str:
    return f"memory://caravel_checkpoints/{label}_{uuid.uuid4().hex}"


def test_full_and_selective_runs_commit_records_on_memory() -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    base = _memory_root("full_selective")
    plugin = CheckpointPlugin(metadata_root=f"{base}/meta")

    run(pipeline, run_root=f"{base}/runs", plugins=[plugin])
    record = plugin.read_record(BRONZE_NODE_ID)
    assert record is not None
    assert record["partition_keys"] == ["a", "b"]

    run(pipeline, run_root=f"{base}/runs", only_stage="silver", plugins=[plugin])


def test_interrupted_replacement_is_uncommitted_on_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    base = _memory_root("interrupted")
    plugin = CheckpointPlugin(metadata_root=f"{base}/meta")

    run(pipeline, run_root=f"{base}/runs", plugins=[plugin])
    _arm_failpoint(monkeypatch, "before_record_write")
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=f"{base}/runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=f"{base}/runs", only_stage="silver", plugins=[plugin])


def test_empty_partitioned_checkpoint_round_trips_on_memory() -> None:
    loader = _SeedLoader({})
    pipeline = _make_pipeline(loader, bronze_allow_empty=True)
    base = _memory_root("empty")
    plugin = CheckpointPlugin(metadata_root=f"{base}/meta")

    run(pipeline, run_root=f"{base}/runs", plugins=[plugin])

    record = plugin.read_record(BRONZE_NODE_ID)
    assert record is not None
    assert record["count"] == 0

    run(pipeline, run_root=f"{base}/runs", only_stage="silver", plugins=[plugin])
