"""Contract tests for the first-party OwnershipPlugin."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from caravel import (
    CheckpointIntegrityError,
    JSONDataset,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    step,
)
from caravel.plugins import (
    CheckpointPlugin,
    OwnershipIntegrityError,
    OwnershipPlugin,
    UnsupportedInventoryVersionError,
    validate_inventory,
    validate_plugins,
)
from caravel.plugins import ownership as ownership_module
from caravel.runner import run


class _InjectedFailure(Exception):
    """Raised by armed failpoints to simulate an interrupted process."""


class _SeedLoader:
    def __init__(self, partitions: dict[str, dict[str, Any]]) -> None:
        self.name = "seed"
        self.partitions = partitions

    def load(self) -> dict[str, dict[str, Any]]:
        return {key: dict(record) for key, record in self.partitions.items()}


def _step_named(step_name: str, dataset_name: str, *, persist: bool = True) -> Any:
    @step(output=PartitionedJSONDataset(name=dataset_name, allow_empty=True), persist=persist)
    def mapper(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        return {key: dict(record) for key, record in partitions.items()}

    mapper.__name__ = step_name
    return mapper


def _two_step_pipeline(
    *, second_step: bool = True, stage_root: Path | str | None = None
) -> Pipeline:
    entries = [_step_named("first_map", "first_map")]
    if second_step:
        entries.append(_step_named("second_map", "second_map"))

    @step(output=JSONDataset(name="summary"))
    def summary(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, int]:
        _ = context
        return {"count": len(partitions)}

    return Pipeline(
        name="own_demo",
        loader=_SeedLoader({"a": {"id": "a"}}),
        stages=[
            Stage(name="bronze", entries=entries, stage_root=stage_root),
            Stage(name="silver", entries=[summary]),
        ],
    )


def _plugin(tmp_path: Path) -> OwnershipPlugin:
    return OwnershipPlugin(metadata_root=tmp_path / "own_meta")


def _pipeline_root(tmp_path: Path) -> Path:
    return tmp_path / "runs" / "own_demo"


def _inventory_file(tmp_path: Path) -> Path:
    return tmp_path / "own_meta" / "v1" / "inventories" / "own_demo.json"


def _arm_failpoint(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def hook(fired: str) -> None:
        if fired == name:
            raise _InjectedFailure(fired)

    monkeypatch.setattr(ownership_module, "_failpoint_hook", hook)


def _disarm_failpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ownership_module, "_failpoint_hook", None)


# ---------------------------------------------------------------------------
# Plugin configuration and schema validation
# ---------------------------------------------------------------------------


def test_metadata_root_is_required_and_explicit() -> None:
    with pytest.raises(TypeError):
        OwnershipPlugin()  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="explicit metadata_root"):
        OwnershipPlugin(metadata_root="")


def test_ownership_plugin_provides_only_the_ownership_capability(tmp_path: Path) -> None:
    plugin_set = validate_plugins((_plugin(tmp_path),))
    assert plugin_set.ownership is not None
    assert plugin_set.checkpoint is None
    assert plugin_set.observers == ()
    assert plugin_set.guards == ()


def test_only_one_ownership_capability_allowed(tmp_path: Path) -> None:
    first = OwnershipPlugin(metadata_root=tmp_path / "a", plugin_id="own-a")
    second = OwnershipPlugin(metadata_root=tmp_path / "b", plugin_id="own-b")
    with pytest.raises(ValueError, match="ownership capability"):
        validate_plugins((first, second))


def _valid_inventory() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "pipeline": "own_demo",
        "run_id": "0f" * 16,
        "managed_root": "/runs/own_demo",
        "output_paths": ["/runs/own_demo/_001_bronze/_001_first_map"],
        "created_at": "2026-07-04T00:00:00.000000Z",
    }


def test_valid_inventory_round_trips_validation() -> None:
    inventory = _valid_inventory()
    assert validate_inventory(inventory, expected_pipeline="own_demo") == inventory


def test_unsupported_inventory_version_raises() -> None:
    inventory = _valid_inventory()
    inventory["schema_version"] = 99
    with pytest.raises(UnsupportedInventoryVersionError, match="version 99"):
        validate_inventory(inventory, expected_pipeline="own_demo")


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda r: r.update({"schema_version": "1"}), "schema_version"),
        (lambda r: r.update({"extra": True}), "unknown"),
        (lambda r: r.pop("managed_root"), "missing"),
        (lambda r: r.update({"pipeline": "other"}), "does not match"),
        (lambda r: r.update({"run_id": "UPPER" * 7}), "run_id"),
        (lambda r: r.update({"output_paths": ["b", "a"]}), "sorted"),
        (lambda r: r.update({"output_paths": [""]}), "non-empty"),
        (lambda r: r.update({"created_at": "yesterday"}), "created_at"),
    ],
)
def test_malformed_inventories_are_rejected(mutate: Any, match: str) -> None:
    inventory = _valid_inventory()
    mutate(inventory)
    with pytest.raises(ValueError, match=match):
        validate_inventory(inventory, expected_pipeline="own_demo")


# ---------------------------------------------------------------------------
# Inventory recording and pruning on full runs
# ---------------------------------------------------------------------------


def test_full_run_records_managed_persisted_outputs(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(), run_root=tmp_path / "runs", plugins=[plugin])

    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None
    root = _pipeline_root(tmp_path)
    assert inventory["managed_root"] == str(root)
    assert inventory["output_paths"] == [
        str(root / "_001_bronze" / "_001_first_map"),
        str(root / "_001_bronze" / "_002_second_map"),
        str(root / "_002_silver" / "_001_summary"),
    ]


def test_removed_step_output_is_pruned_on_next_full_run(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])
    stale_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"
    assert stale_dir.exists()

    run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])

    assert not stale_dir.exists()
    assert (_pipeline_root(tmp_path) / "_001_bronze" / "_001_first_map").exists()
    assert plugin.last_reconciliation is not None
    assert plugin.last_reconciliation.pruned_paths == (str(stale_dir),)


def test_renamed_stage_and_step_outputs_are_pruned(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)

    def _pipeline(stage_name: str, step_name: str) -> Pipeline:
        return Pipeline(
            name="own_demo",
            loader=_SeedLoader({"a": {"id": "a"}}),
            stages=[Stage(name=stage_name, entries=[_step_named(step_name, "mapped")])],
        )

    run(_pipeline("bronze", "first_map"), run_root=tmp_path / "runs", plugins=[plugin])
    old_stage_dir = _pipeline_root(tmp_path) / "_001_bronze"
    assert (old_stage_dir / "_001_first_map").exists()

    run(_pipeline("bronzeX", "renamed_map"), run_root=tmp_path / "runs", plugins=[plugin])

    assert not (old_stage_dir / "_001_first_map").exists()
    assert (_pipeline_root(tmp_path) / "_001_bronzeX" / "_001_renamed_map").exists()


def test_persist_flip_prunes_previous_output(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)

    def _pipeline(persist: bool) -> Pipeline:
        return Pipeline(
            name="own_demo",
            loader=_SeedLoader({"a": {"id": "a"}}),
            stages=[
                Stage(
                    name="bronze",
                    entries=[
                        _step_named("first_map", "first_map"),
                        _step_named("second_map", "second_map", persist=persist),
                    ],
                )
            ],
        )

    run(_pipeline(persist=True), run_root=tmp_path / "runs", plugins=[plugin])
    stale_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"
    assert stale_dir.exists()

    run(_pipeline(persist=False), run_root=tmp_path / "runs", plugins=[plugin])
    assert not stale_dir.exists()


def test_removed_route_outputs_are_pruned(tmp_path: Path) -> None:
    from caravel import Branch

    plugin = _plugin(tmp_path)

    def _pipeline(with_text_route: bool) -> Pipeline:
        routes: dict[str, list[Any]] = {"json": [_step_named("parse_json", "parsed")]}
        if with_text_route:
            routes["text"] = [_step_named("decode_text", "decoded")]
        branch = Branch(name="by_source", by="source", routes=routes)
        partitions = {"x": {"__source__": "json"}}
        if with_text_route:
            partitions["y"] = {"__source__": "text"}
        return Pipeline(
            name="own_demo",
            loader=_SeedLoader(partitions),
            stages=[Stage(name="bronze", entries=[branch])],
        )

    run(_pipeline(with_text_route=True), run_root=tmp_path / "runs", plugins=[plugin])
    text_route_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_001_by_source" / "text"
    assert (text_route_dir / "decode_text").exists()

    run(_pipeline(with_text_route=False), run_root=tmp_path / "runs", plugins=[plugin])
    assert not (text_route_dir / "decode_text").exists()
    json_route_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_001_by_source" / "json"
    assert (json_route_dir / "parse_json").exists()


def test_foreign_files_and_matching_looking_directories_survive(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])

    root = _pipeline_root(tmp_path)
    foreign_file = root / "notes.txt"
    foreign_file.write_text("user data", "utf-8")
    lookalike = root / "_009_fake_stage" / "_001_fake_step"
    lookalike.mkdir(parents=True)
    (lookalike / "data.json").write_text("{}", "utf-8")

    run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])

    assert foreign_file.exists()
    assert (lookalike / "data.json").exists()
    assert not (root / "_001_bronze" / "_002_second_map").exists()


def test_no_prior_inventory_means_no_implicit_deletion(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    stale_looking = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"
    stale_looking.mkdir(parents=True)
    (stale_looking / "old.json").write_text("{}", "utf-8")

    run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])

    assert (stale_looking / "old.json").exists()


def test_stage_root_trees_are_never_pruned(tmp_path: Path) -> None:
    external_root = tmp_path / "external_bronze"
    plugin = _plugin(tmp_path)

    run(
        _two_step_pipeline(second_step=True, stage_root=external_root),
        run_root=tmp_path / "runs",
        plugins=[plugin],
    )
    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None
    assert all(path.startswith(str(_pipeline_root(tmp_path))) for path in inventory["output_paths"])
    assert (external_root / "_002_second_map").exists()

    run(
        _two_step_pipeline(second_step=False, stage_root=external_root),
        run_root=tmp_path / "runs",
        plugins=[plugin],
    )
    assert (external_root / "_002_second_map").exists()


def test_selective_runs_never_prune_or_replace_inventory(tmp_path: Path) -> None:
    ownership = _plugin(tmp_path)
    checkpoint = CheckpointPlugin(metadata_root=tmp_path / "ckpt_meta")
    pipeline = _two_step_pipeline(second_step=True)

    run(pipeline, run_root=tmp_path / "runs", plugins=[checkpoint, ownership])
    first_inventory = json.loads(_inventory_file(tmp_path).read_text("utf-8"))

    run(
        pipeline,
        run_root=tmp_path / "runs",
        only_stage="silver",
        plugins=[checkpoint, ownership],
    )

    assert ownership.last_reconciliation is not None
    assert ownership.last_reconciliation.selective is True
    assert ownership.last_reconciliation.inventory_replaced is False
    assert ownership.last_reconciliation.pruned_paths == ()
    assert json.loads(_inventory_file(tmp_path).read_text("utf-8")) == first_inventory


def test_changed_managed_root_is_not_a_deletion_basis(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])
    old_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"
    assert old_dir.exists()

    run(_two_step_pipeline(second_step=False), run_root=tmp_path / "other_runs", plugins=[plugin])

    assert old_dir.exists()
    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None
    assert inventory["managed_root"] == str(tmp_path / "other_runs" / "own_demo")


# ---------------------------------------------------------------------------
# Malformed, tampered, and interrupted reconciliation
# ---------------------------------------------------------------------------


def test_malformed_inventory_means_no_deletion(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])
    stale_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"

    _inventory_file(tmp_path).write_text("{not json", "utf-8")
    run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])

    assert stale_dir.exists()
    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None


def test_unsupported_inventory_version_fails_the_run(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])

    inventory = json.loads(_inventory_file(tmp_path).read_text("utf-8"))
    inventory["schema_version"] = 99
    _inventory_file(tmp_path).write_text(json.dumps(inventory), "utf-8")

    with pytest.raises(UnsupportedInventoryVersionError):
        run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])


def test_traversal_candidate_aborts_before_any_deletion(tmp_path: Path) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    (outside_dir / "keep.txt").write_text("keep", "utf-8")
    stale_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"

    inventory = json.loads(_inventory_file(tmp_path).read_text("utf-8"))
    escape = str(_pipeline_root(tmp_path) / ".." / ".." / "outside")
    inventory["output_paths"] = sorted(set(inventory["output_paths"] + [escape]))
    _inventory_file(tmp_path).write_text(json.dumps(inventory), "utf-8")

    with pytest.raises(OwnershipIntegrityError, match="escapes the managed root"):
        run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])

    assert (outside_dir / "keep.txt").exists()
    assert stale_dir.exists()


@pytest.mark.parametrize(
    "failpoint_name",
    ["before_deletion", "during_deletion", "before_inventory_commit"],
)
def test_interrupted_reconciliation_converges_on_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failpoint_name: str
) -> None:
    plugin = _plugin(tmp_path)
    run(_two_step_pipeline(second_step=True), run_root=tmp_path / "runs", plugins=[plugin])
    stale_dir = _pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map"

    _arm_failpoint(monkeypatch, failpoint_name)
    with pytest.raises(_InjectedFailure):
        run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])
    _disarm_failpoints(monkeypatch)

    run(_two_step_pipeline(second_step=False), run_root=tmp_path / "runs", plugins=[plugin])

    assert not stale_dir.exists()
    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None
    assert str(stale_dir) not in inventory["output_paths"]


# ---------------------------------------------------------------------------
# Cross-plugin consistency with CheckpointPlugin
# ---------------------------------------------------------------------------


def test_checkpoint_evidence_for_pruned_output_cannot_authorize_reuse(tmp_path: Path) -> None:
    checkpoint = CheckpointPlugin(metadata_root=tmp_path / "ckpt_meta")
    ownership = _plugin(tmp_path)
    v1 = _two_step_pipeline(second_step=True)

    run(v1, run_root=tmp_path / "runs", plugins=[checkpoint, ownership])
    assert checkpoint.read_record("stage-001-entry-002") is not None

    run(
        _two_step_pipeline(second_step=False),
        run_root=tmp_path / "runs",
        plugins=[ownership],
    )
    assert not (_pipeline_root(tmp_path) / "_001_bronze" / "_002_second_map").exists()

    with pytest.raises(CheckpointIntegrityError):
        run(v1, run_root=tmp_path / "runs", only_stage="silver", plugins=[checkpoint, ownership])


def test_inventory_contains_no_secrets(tmp_path: Path) -> None:
    canary = "canary-secret-value"
    plugin = OwnershipPlugin(
        metadata_root=tmp_path / "own_meta",
        storage_options={"secret": canary},
    )
    run(
        _two_step_pipeline(second_step=True),
        run_root=tmp_path / "runs",
        plugins=[plugin],
        params={"token": canary},
    )

    inventory_text = _inventory_file(tmp_path).read_text("utf-8")
    assert canary not in inventory_text
    assert set(json.loads(inventory_text)) == {
        "schema_version",
        "pipeline",
        "run_id",
        "managed_root",
        "output_paths",
        "created_at",
    }


# ---------------------------------------------------------------------------
# memory:// contract coverage
# ---------------------------------------------------------------------------


def test_pruning_and_inventory_replacement_on_memory() -> None:
    base = f"memory://caravel_ownership/{uuid.uuid4().hex}"
    plugin = OwnershipPlugin(metadata_root=f"{base}/meta")

    run(_two_step_pipeline(second_step=True), run_root=f"{base}/runs", plugins=[plugin])
    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None
    assert any(path.endswith("_002_second_map") for path in inventory["output_paths"])

    run(_two_step_pipeline(second_step=False), run_root=f"{base}/runs", plugins=[plugin])

    import fsspec

    fs = fsspec.filesystem("memory")
    stale = f"{base.removeprefix('memory://')}/runs/own_demo/_001_bronze/_002_second_map"
    assert not fs.exists(stale)
    inventory = plugin.read_inventory("own_demo")
    assert inventory is not None
    assert not any(path.endswith("_002_second_map") for path in inventory["output_paths"])
