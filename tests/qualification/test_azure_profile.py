"""Credential-gated production-profile contracts on Azure Blob Storage."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

import pytest

from caravel import (
    Branch,
    JSONDataset,
    MissingPriorOutputError,
    PartitionedJSONDataset,
    Pipeline,
    Stage,
    Step,
)
from caravel.plugins import (
    CheckpointPlugin,
    LeaseHeldError,
    LeasePlugin,
    OwnershipPlugin,
    RunEvidencePlugin,
    RunFacts,
    RunOutcome,
)
from caravel.plugins import checkpoint as checkpoint_module
from caravel.runner import run
from caravel.storage import ensure_parent_dir, join_path, resolve_fs


class _InjectedInterruption(Exception):
    pass


class _SeedLoader:
    def __init__(self, partitions: dict[str, dict[str, Any]]) -> None:
        self.name = "azure-qualification-seed"
        self.partitions = partitions

    def load(self) -> dict[str, dict[str, Any]]:
        return {key: dict(value) for key, value in self.partitions.items()}


def _azure_options() -> dict[str, str]:
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
    sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN", "").strip()
    tenant_id = os.environ.get("AZURE_TENANT_ID", "").strip()
    client_id = os.environ.get("AZURE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "").strip()
    if not account_name:
        pytest.skip("Azure qualification credentials are not configured")
    options = {"account_name": account_name}
    if account_key:
        options["account_key"] = account_key
    elif sas_token:
        options["sas_token"] = sas_token
    elif tenant_id and client_id and client_secret:
        options.update(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    else:
        pytest.skip("Azure qualification credentials are not configured")
    return options


@pytest.fixture(scope="session")
def azure_root() -> str:
    root = os.environ.get("CARAVEL_AZURE_TEST_ROOT", "").strip().rstrip("/")
    if not root:
        pytest.skip("CARAVEL_AZURE_TEST_ROOT is not configured")
    if not root.startswith(("abfs://", "az://")):
        pytest.fail("CARAVEL_AZURE_TEST_ROOT must be an abfs:// or az:// URL")
    return root


@pytest.fixture(scope="session")
def azure_options() -> Mapping[str, Any]:
    return _azure_options()


def _exists(path: str, options: Mapping[str, Any]) -> bool:
    fs, resolved = resolve_fs(path, options)
    return bool(fs.exists(resolved))


def _write_json(path: str, payload: object, options: Mapping[str, Any]) -> None:
    fs, resolved = resolve_fs(path, options)
    ensure_parent_dir(fs, resolved)
    with fs.open(resolved, mode="wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _read_json(path: str, options: Mapping[str, Any]) -> dict[str, Any]:
    fs, resolved = resolve_fs(path, options)
    with fs.open(resolved, mode="rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert isinstance(payload, dict)
    return payload


def _pipeline(
    name: str,
    loader: _SeedLoader,
    options: Mapping[str, Any],
    *,
    include_second_step: bool = True,
    unsafe_key: bool = False,
) -> Pipeline:
    def map_records(
        partitions: dict[str, dict[str, Any]], *, context: object
    ) -> dict[str, dict[str, Any]]:
        _ = context
        if unsafe_key:
            return {"../escape": {"unsafe": True}}
        return {key: {**value, "mapped": True} for key, value in partitions.items()}

    entries: list[Step | Branch | Callable[..., Any]] = [
        Step(
            fn=map_records,
            name="map_records",
            output=PartitionedJSONDataset(
                name="map_records",
                storage_options=options,
                allow_empty=True,
            ),
        )
    ]

    if include_second_step:

        def summarize(partitions: dict[str, dict[str, Any]], *, context: object) -> dict[str, int]:
            _ = context
            return {"count": len(partitions)}

        entries.append(
            Step(
                fn=summarize,
                name="summarize",
                output=JSONDataset(name="summarize", storage_options=options),
            )
        )

    return Pipeline(name=name, loader=loader, stages=[Stage(name="bronze", entries=entries)])


def test_full_selective_events_and_normal_lease_cleanup(
    azure_root: str, azure_options: Mapping[str, Any]
) -> None:
    case = join_path(azure_root, "full-selective")
    output = join_path(case, "output")
    metadata = join_path(case, "metadata")
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _pipeline("azure_full_selective", loader, azure_options)
    checkpoint = CheckpointPlugin(
        metadata_root=join_path(metadata, "checkpoints"), storage_options=azure_options
    )
    ownership = OwnershipPlugin(
        metadata_root=join_path(metadata, "ownership"), storage_options=azure_options
    )
    evidence = RunEvidencePlugin(
        metadata_root=join_path(metadata, "evidence"), storage_options=azure_options
    )
    lease = LeasePlugin(
        metadata_root=join_path(metadata, "leases"),
        storage_options=azure_options,
        heartbeat_interval=0.1,
        stale_threshold=1.0,
    )
    plugins = [checkpoint, ownership, evidence, lease]

    run(pipeline, run_root=output, plugins=plugins)
    run(pipeline, run_root=output, only_step="summarize", only_stage="bronze", plugins=plugins)

    assert checkpoint.read_record("stage-001-entry-001") is not None
    assert ownership.read_inventory(pipeline.name) is not None
    assert not _exists(lease.lease_path(pipeline.name), azure_options)

    fs, events_root = resolve_fs(join_path(metadata, "evidence", "v1", "events"), azure_options)
    event_payloads = []
    for event_path in fs.find(events_root, withdirs=False):
        with fs.open(event_path, mode="rt", encoding="utf-8") as handle:
            event_payloads.append(json.load(handle))
    kinds = {event["kind"] for event in event_payloads}
    assert {"run_started", "node_skipped", "node_completed", "run_completed"} <= kinds


def test_empty_checkpoint_reuses_without_a_durable_directory(
    azure_root: str, azure_options: Mapping[str, Any]
) -> None:
    case = join_path(azure_root, "committed-empty")
    output = join_path(case, "output")
    metadata = join_path(case, "metadata")
    pipeline_name = "azure_committed_empty"
    pipeline = _pipeline(pipeline_name, _SeedLoader({}), azure_options)
    checkpoint = CheckpointPlugin(metadata_root=metadata, storage_options=azure_options)

    run(pipeline, run_root=output, plugins=[checkpoint])
    record = checkpoint.read_record("stage-001-entry-001")
    assert record is not None
    assert record["partition_keys"] == []

    empty_dir = join_path(output, pipeline_name, "_001_bronze", "_001_map_records")
    fs, resolved_empty_dir = resolve_fs(empty_dir, azure_options)
    if fs.exists(resolved_empty_dir):
        fs.rm(resolved_empty_dir, recursive=True)
    assert not _exists(empty_dir, azure_options)

    run(
        pipeline,
        run_root=output,
        only_stage="bronze",
        only_step="summarize",
        plugins=[checkpoint],
    )
    summary_path = join_path(
        output,
        pipeline_name,
        "_001_bronze",
        "_002_summarize",
        "_002_summarize.json",
    )
    assert _read_json(summary_path, azure_options) == {"count": 0}


def test_interrupted_replacement_invalidates_then_recovers(
    azure_root: str, azure_options: Mapping[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    case = join_path(azure_root, "interrupted-replacement")
    output = join_path(case, "output")
    metadata = join_path(case, "metadata")
    loader = _SeedLoader({"a": {"version": 1}})
    pipeline = _pipeline("azure_interruption", loader, azure_options)
    checkpoint = CheckpointPlugin(metadata_root=metadata, storage_options=azure_options)

    run(pipeline, run_root=output, plugins=[checkpoint])

    def interrupt(name: str) -> None:
        if name == "after_record_invalidation":
            raise _InjectedInterruption(name)

    loader.partitions = {"a": {"version": 2}}
    monkeypatch.setattr(checkpoint_module, "_failpoint_hook", interrupt)
    with pytest.raises(_InjectedInterruption):
        run(pipeline, run_root=output, plugins=[checkpoint])
    monkeypatch.setattr(checkpoint_module, "_failpoint_hook", None)

    assert checkpoint.read_record("stage-001-entry-001") is None
    with pytest.raises(MissingPriorOutputError):
        run(
            pipeline,
            run_root=output,
            only_stage="bronze",
            only_step="summarize",
            plugins=[checkpoint],
        )

    run(pipeline, run_root=output, plugins=[checkpoint])
    run(
        pipeline,
        run_root=output,
        only_stage="bronze",
        only_step="summarize",
        plugins=[checkpoint],
    )


def test_invalid_checkpoint_evidence_fails_closed(
    azure_root: str, azure_options: Mapping[str, Any]
) -> None:
    case = join_path(azure_root, "invalid-evidence")
    output = join_path(case, "output")
    metadata = join_path(case, "metadata")
    pipeline = _pipeline("azure_invalid_evidence", _SeedLoader({"a": {"id": "a"}}), azure_options)
    checkpoint = CheckpointPlugin(metadata_root=metadata, storage_options=azure_options)

    run(pipeline, run_root=output, plugins=[checkpoint])
    _write_json(checkpoint.record_path("stage-001-entry-001"), {"corrupt": True}, azure_options)

    with pytest.raises(MissingPriorOutputError):
        run(
            pipeline,
            run_root=output,
            only_stage="bronze",
            only_step="summarize",
            plugins=[checkpoint],
        )


def test_ownership_prunes_removed_output(azure_root: str, azure_options: Mapping[str, Any]) -> None:
    case = join_path(azure_root, "ownership-pruning")
    output = join_path(case, "output")
    metadata = join_path(case, "metadata")
    loader = _SeedLoader({"a": {"id": "a"}})
    ownership = OwnershipPlugin(metadata_root=metadata, storage_options=azure_options)
    pipeline_name = "azure_ownership"

    run(
        _pipeline(pipeline_name, loader, azure_options),
        run_root=output,
        plugins=[ownership],
    )
    stale = join_path(output, pipeline_name, "_001_bronze", "_002_summarize")
    assert _exists(stale, azure_options)

    run(
        _pipeline(pipeline_name, loader, azure_options, include_second_step=False),
        run_root=output,
        plugins=[ownership],
    )
    assert not _exists(stale, azure_options)


def test_live_and_stale_lease_classification(
    azure_root: str, azure_options: Mapping[str, Any]
) -> None:
    metadata = join_path(azure_root, "lease-classification", "metadata")
    pipeline_name = "azure_lease"
    run_facts = RunFacts(
        pipeline_name=pipeline_name,
        run_id="1" * 32,
        run_root=join_path(azure_root, "lease-classification", "output"),
        is_selective=False,
    )
    first = LeasePlugin(
        metadata_root=metadata,
        storage_options=azure_options,
        heartbeat_interval=0.1,
        stale_threshold=1.0,
        holder="qualification-first",
    )
    second = LeasePlugin(
        metadata_root=metadata,
        storage_options=azure_options,
        heartbeat_interval=0.1,
        stale_threshold=1.0,
        holder="qualification-second",
    )

    first.enter(run_facts)
    try:
        with pytest.raises(LeaseHeldError):
            second.enter(run_facts)
    finally:
        first.exit(run_facts, RunOutcome(status="completed"))

    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    abandoned_run_id = "2" * 32
    _write_json(
        first.lease_path(pipeline_name),
        {
            "schema_version": 1,
            "pipeline": pipeline_name,
            "run_id": abandoned_run_id,
            "holder": "abandoned-holder",
            "host": "abandoned-host",
            "pid": 4242,
            "started_at": stale_at,
            "heartbeat_at": stale_at,
        },
        azure_options,
    )

    replacement_facts = RunFacts(
        pipeline_name=pipeline_name,
        run_id="3" * 32,
        run_root=run_facts.run_root,
        is_selective=False,
    )
    second.enter(replacement_facts)
    try:
        recovery_path = second.recovery_path(pipeline_name, replacement_facts.run_id)
        assert _exists(recovery_path, azure_options)
        recovery = _read_json(recovery_path, azure_options)
        assert recovery["recovered_by_run_id"] == replacement_facts.run_id
        assert recovery["abandoned_lease"]["run_id"] == abandoned_run_id
    finally:
        second.exit(replacement_facts, RunOutcome(status="completed"))


def test_remote_replacement_removes_stale_partitions_and_rejects_unsafe_keys(
    azure_root: str, azure_options: Mapping[str, Any]
) -> None:
    replacement_case = join_path(azure_root, "replacement-cleanup")
    output = join_path(replacement_case, "output")
    loader = _SeedLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _pipeline("azure_cleanup", loader, azure_options, include_second_step=False)
    run(pipeline, run_root=output)

    stale_partition = join_path(
        output, "azure_cleanup", "_001_bronze", "_001_map_records", "b.json"
    )
    assert _exists(stale_partition, azure_options)
    loader.partitions = {"a": {"id": "a"}}
    run(pipeline, run_root=output)
    assert not _exists(stale_partition, azure_options)

    unsafe_case = join_path(azure_root, "unsafe-path")
    unsafe_pipeline = _pipeline(
        "azure_unsafe",
        _SeedLoader({"a": {"id": "a"}}),
        azure_options,
        include_second_step=False,
        unsafe_key=True,
    )
    with pytest.raises(ValueError, match="unsafe|traversal|segment"):
        run(unsafe_pipeline, run_root=join_path(unsafe_case, "output"))
    escaped_target = join_path(
        unsafe_case,
        "output",
        "azure_unsafe",
        "_001_bronze",
        "escape.json",
    )
    assert not _exists(escaped_target, azure_options)
