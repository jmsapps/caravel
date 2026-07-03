"""Contract tests for the central checkpoint registry"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    CheckpointIntegrityError,
    JSONDataset,
    MissingPriorOutputError,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    UnsupportedCheckpointVersionError,
    step,
)
from caravel import storage as storage_module
from caravel.runner import run
from caravel.storage import (
    CHECKPOINTS_DIRNAME,
    METADATA_DIRNAME,
    checkpoint_record_path,
    read_checkpoint_record,
    validate_checkpoint_record,
)

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


def _arm_failpoint(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def hook(fired: str) -> None:
        if fired == name:
            raise _InjectedFailure(fired)

    monkeypatch.setattr(storage_module, "_failpoint_hook", hook)


def _disarm_failpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage_module, "_failpoint_hook", None)


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
# Commit protocol: layout, provenance, and reservation
# ---------------------------------------------------------------------------


def test_step_directories_contain_dataset_files_only(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}}))

    run(pipeline, run_root=tmp_path)

    bronze_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_001_bronze_map"
    silver_dir = _pipeline_root(tmp_path) / "_002_silver" / "_001_silver_summary"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]
    assert sorted(p.name for p in silver_dir.iterdir()) == ["_001_silver_summary.json"]

    checkpoints_dir = _pipeline_root(tmp_path) / METADATA_DIRNAME / CHECKPOINTS_DIRNAME
    assert sorted(p.name for p in checkpoints_dir.iterdir()) == [
        f"{BRONZE_NODE_ID}.json",
        f"{SILVER_NODE_ID}.json",
    ]


def test_committed_records_share_one_run_id_and_validate(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))

    run(pipeline, run_root=tmp_path)

    bronze = read_checkpoint_record(_pipeline_root(tmp_path), BRONZE_NODE_ID)
    silver = read_checkpoint_record(_pipeline_root(tmp_path), SILVER_NODE_ID)
    assert bronze is not None and silver is not None
    assert bronze["run_id"] == silver["run_id"]
    assert bronze["partition_keys"] == ["a"]
    assert bronze["count"] == 1
    assert silver["partition_keys"] is None
    assert silver["count"] == 1
    assert bronze["output_path"].endswith("_001_bronze/_001_bronze_map")


def test_rerun_replaces_records_with_new_run_id(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))

    run(pipeline, run_root=tmp_path)
    first = read_checkpoint_record(_pipeline_root(tmp_path), BRONZE_NODE_ID)
    run(pipeline, run_root=tmp_path)
    second = read_checkpoint_record(_pipeline_root(tmp_path), BRONZE_NODE_ID)

    assert first is not None and second is not None
    assert first["run_id"] != second["run_id"]


def test_metadata_directory_survives_clean_dirs(tmp_path: Path) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    pipeline.stages[0].clean_dirs = True

    run(pipeline, run_root=tmp_path)
    run(pipeline, run_root=tmp_path)

    assert (_pipeline_root(tmp_path) / METADATA_DIRNAME / CHECKPOINTS_DIRNAME).exists()
    assert read_checkpoint_record(_pipeline_root(tmp_path), BRONZE_NODE_ID) is not None


def test_fewer_key_replacement_leaves_no_stale_partitions(tmp_path: Path) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)

    run(pipeline, run_root=tmp_path)
    loader.partitions = {"a": {"id": "a"}}
    run(pipeline, run_root=tmp_path)

    bronze_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_001_bronze_map"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json"]

    run(pipeline, run_root=tmp_path, only_stage="silver")
    assert _silver_payload(tmp_path) == {"count": 1}


def test_empty_partitioned_checkpoint_round_trips_without_sentinel(tmp_path: Path) -> None:
    loader = _SeedLoader({})
    pipeline = _make_pipeline(loader, bronze_allow_empty=True)

    run(pipeline, run_root=tmp_path)

    record = read_checkpoint_record(_pipeline_root(tmp_path), BRONZE_NODE_ID)
    assert record is not None
    assert record["partition_keys"] == []
    assert record["count"] == 0
    bronze_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_001_bronze_map"
    assert list(bronze_dir.iterdir()) == []

    run(pipeline, run_root=tmp_path, only_stage="silver")
    assert _silver_payload(tmp_path) == {"count": 0}


def test_stage_root_output_is_checked_from_central_metadata(tmp_path: Path) -> None:
    external_root = tmp_path / "external_bronze"
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader, bronze_stage_root=external_root)

    run(pipeline, run_root=tmp_path / "runs")

    record = read_checkpoint_record(_pipeline_root(tmp_path / "runs"), BRONZE_NODE_ID)
    assert record is not None
    assert record["output_path"] == (external_root / "_001_bronze_map").as_posix()

    run(pipeline, run_root=tmp_path / "runs", only_stage="silver")
    assert _silver_payload(tmp_path / "runs") == {"count": 1}


def test_output_without_central_record_is_not_a_checkpoint(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))

    bronze_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_001_bronze_map"
    PartitionedJSONDataset(name="bronze_map").save({"a": {"id": "a", "mapped": True}}, bronze_dir)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path, only_stage="silver")


# ---------------------------------------------------------------------------
# Interrupted replacement via deterministic failpoints
# ---------------------------------------------------------------------------


def test_failure_before_invalidation_preserves_prior_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    run(pipeline, run_root=tmp_path)

    loader.partitions = {"a": {"id": "a"}}
    _arm_failpoint(monkeypatch, "before_record_invalidation")
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path)
    _disarm_failpoints(monkeypatch)

    run(pipeline, run_root=tmp_path, only_stage="silver")
    assert _silver_payload(tmp_path) == {"count": 2}


@pytest.mark.parametrize(
    "failpoint_name",
    [
        "after_record_invalidation",
        "after_output_cleanup",
        "after_output_file_written:1",
        "before_record_write",
    ],
)
def test_failure_after_invalidation_leaves_node_uncommitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failpoint_name: str
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    run(pipeline, run_root=tmp_path)

    _arm_failpoint(monkeypatch, failpoint_name)
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path)
    _disarm_failpoints(monkeypatch)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path, only_stage="silver")


def test_failure_during_record_write_commits_via_rediscovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exception after the record bytes land is resolved by rediscovery."""
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)

    _arm_failpoint(monkeypatch, "after_record_write")
    run(pipeline, run_root=tmp_path)
    _disarm_failpoints(monkeypatch)

    assert read_checkpoint_record(_pipeline_root(tmp_path), BRONZE_NODE_ID) is not None
    run(pipeline, run_root=tmp_path, only_stage="silver")
    assert _silver_payload(tmp_path) == {"count": 1}


def test_failure_after_record_verified_still_committed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)

    _arm_failpoint(monkeypatch, "after_record_verified")
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=tmp_path)
    _disarm_failpoints(monkeypatch)

    run(pipeline, run_root=tmp_path, only_stage="silver")
    assert _silver_payload(tmp_path) == {"count": 1}


# ---------------------------------------------------------------------------
# Selective-boundary verification against tampered or inconsistent state
# ---------------------------------------------------------------------------


def _bronze_record_file(run_root: Path) -> Path:
    return Path(checkpoint_record_path(_pipeline_root(run_root), BRONZE_NODE_ID))


def test_malformed_record_means_no_checkpoint(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    run(pipeline, run_root=tmp_path)

    _bronze_record_file(tmp_path).write_text("{not json", encoding="utf-8")

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=tmp_path, only_stage="silver")


def test_unsupported_record_version_raises_at_boundary(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    run(pipeline, run_root=tmp_path)

    record_file = _bronze_record_file(tmp_path)
    record = json.loads(record_file.read_text("utf-8"))
    record["schema_version"] = 99
    record_file.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(UnsupportedCheckpointVersionError):
        run(pipeline, run_root=tmp_path, only_stage="silver")


def test_record_declaration_mismatch_raises_integrity_error(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    run(pipeline, run_root=tmp_path)

    record_file = _bronze_record_file(tmp_path)
    record = json.loads(record_file.read_text("utf-8"))
    record["dataset"] = {"type": "PartitionedTextDataset", "name": "bronze_map"}
    record_file.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(CheckpointIntegrityError, match="does not match"):
        run(pipeline, run_root=tmp_path, only_stage="silver")


def test_missing_physical_partition_raises_integrity_error(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}}))
    run(pipeline, run_root=tmp_path)

    (_pipeline_root(tmp_path) / "_001_bronze" / "_001_bronze_map" / "b.json").unlink()

    with pytest.raises(CheckpointIntegrityError, match="missing=\\['b'\\]"):
        run(pipeline, run_root=tmp_path, only_stage="silver")


def test_extra_physical_partition_raises_integrity_error(tmp_path: Path) -> None:
    pipeline = _make_pipeline(_SeedLoader({"a": {"id": "a"}}))
    run(pipeline, run_root=tmp_path)

    stray = _pipeline_root(tmp_path) / "_001_bronze" / "_001_bronze_map" / "z.json"
    stray.write_text("{}", encoding="utf-8")

    with pytest.raises(CheckpointIntegrityError, match="extra=\\['z'\\]"):
        run(pipeline, run_root=tmp_path, only_stage="silver")


# ---------------------------------------------------------------------------
# memory:// contract coverage
# ---------------------------------------------------------------------------


def _memory_root(label: str) -> str:
    return f"memory://caravel_checkpoints/{label}_{uuid.uuid4().hex}"


def test_full_and_selective_runs_commit_records_on_memory() -> None:
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_pipeline(loader)
    run_root = _memory_root("full_selective")

    run(pipeline, run_root=run_root)
    record = read_checkpoint_record(f"{run_root}/ckpt_demo", BRONZE_NODE_ID)
    assert record is not None
    assert record["partition_keys"] == ["a", "b"]

    run(pipeline, run_root=run_root, only_stage="silver")


def test_interrupted_replacement_is_uncommitted_on_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _SeedLoader({"a": {"id": "a"}})
    pipeline = _make_pipeline(loader)
    run_root = _memory_root("interrupted")

    run(pipeline, run_root=run_root)
    _arm_failpoint(monkeypatch, "before_record_write")
    with pytest.raises(_InjectedFailure):
        run(pipeline, run_root=run_root)
    _disarm_failpoints(monkeypatch)

    with pytest.raises(MissingPriorOutputError):
        run(pipeline, run_root=run_root, only_stage="silver")


def test_empty_partitioned_checkpoint_round_trips_on_memory() -> None:
    loader = _SeedLoader({})
    pipeline = _make_pipeline(loader, bronze_allow_empty=True)
    run_root = _memory_root("empty")

    run(pipeline, run_root=run_root)

    record = read_checkpoint_record(f"{run_root}/ckpt_demo", BRONZE_NODE_ID)
    assert record is not None
    assert record["count"] == 0

    run(pipeline, run_root=run_root, only_stage="silver")
