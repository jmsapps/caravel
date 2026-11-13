"""The branching example runs on the durable engine.

Routing happens at the branch boundary and is committed per partition; each
record travels exactly one route and merges when that route seals.
"""

from __future__ import annotations

import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from caravel import DurableStorage  # noqa: E402
from caravel.durability import WorkUnitKey  # noqa: E402
from examples.branching.pipeline import (  # noqa: E402
    HTML_SOURCE_NAME,
    JSON_SOURCE_NAME,
    build_branching_pipeline,
    run_branching_pipeline,
)

BRANCH_NODE = "stage-001-entry-001"


def test_the_branching_example_completes(tmp_path: Path) -> None:
    result = run_branching_pipeline(state_root=tmp_path)
    assert result.status == "completed"
    assert result.restart_safe is True


def test_each_record_travels_exactly_one_route(tmp_path: Path) -> None:
    run_branching_pipeline(state_root=tmp_path)
    storage = DurableStorage.at(tmp_path)
    try:
        routes: dict[str, str] = {}
        for partition_id in storage.progress.work_unit_partitions("branching-1", BRANCH_NODE):
            unit = storage.progress.load_work_unit(
                WorkUnitKey("branching-1", BRANCH_NODE, partition_id)
            )
            assert unit is not None and unit.route_key is not None
            routes[partition_id] = unit.route_key

        assert routes, "no partitions were routed"
        assert {routes[key] for key in routes} == {JSON_SOURCE_NAME, HTML_SOURCE_NAME}
        for partition_id, route in routes.items():
            expected = JSON_SOURCE_NAME if partition_id.startswith("json") else HTML_SOURCE_NAME
            assert route == expected
    finally:
        storage.close()


def test_both_sources_converge_through_the_merge(tmp_path: Path) -> None:
    run_branching_pipeline(state_root=tmp_path)
    storage = DurableStorage.at(tmp_path)
    try:
        results = storage.results(build_branching_pipeline(), job_id="branching-1")
        gold = results.of("gold_partition_by_language")
        keys = sorted(key for payload in gold.values() for key in payload)
        assert keys == ["en/html-alpha", "en/json-alpha", "fr/html-beta", "fr/json-beta"]
        origins = {record["origin"] for payload in gold.values() for record in payload.values()}
        assert origins == {"json", "html"}
    finally:
        storage.close()


def test_the_source_tag_does_not_reach_committed_output(tmp_path: Path) -> None:
    from caravel import SOURCE_FIELD

    run_branching_pipeline(state_root=tmp_path)
    storage = DurableStorage.at(tmp_path)
    try:
        results = storage.results(build_branching_pipeline(), job_id="branching-1")
        for payload in results.of("gold_partition_by_language").values():
            for record in payload.values():
                assert SOURCE_FIELD not in record
    finally:
        storage.close()


def test_reinvoking_the_branching_example_reprocesses_nothing(tmp_path: Path) -> None:
    assert run_branching_pipeline(state_root=tmp_path).status == "completed"
    again = run_branching_pipeline(state_root=tmp_path)
    assert again.status == "completed"
    assert again.committed_batches == 0
