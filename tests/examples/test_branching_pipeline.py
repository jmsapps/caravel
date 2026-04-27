import json
import sys
from pathlib import Path

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from examples.branching.pipeline import (
    _DEFAULT_COLLISION_HTML_SOURCE_DIR,
    _DEFAULT_COLLISION_JSON_SOURCE_DIR,
    BRANCH_STEP_NAME,
    BRONZE_ROUTE_HTML_STEP_NAME,
    BRONZE_ROUTE_JSON_STEP_NAME,
    BRONZE_STAGE_NAME,
    GOLD_STAGE_NAME,
    GOLD_STEP_NAME,
    HTML_SOURCE_NAME,
    JSON_SOURCE_NAME,
    PIPELINE_NAME,
    SILVER_STAGE_NAME,
    SILVER_STEP_NAME,
    build_branching_pipeline,
)
from caravel import Branch, KeyCollisionError
from caravel.runner import run


def _partition_file(
    run_root: Path,
    stage_index: int,
    stage_name: str,
    step_index: int,
    step_name: str,
    partition_key: str,
    suffix: str = ".json",
) -> Path:
    return (
        run_root
        / f"_{stage_index:03d}_{stage_name}"
        / f"_{step_index:03d}_{step_name}"
        / Path(f"{partition_key}{suffix}")
    )


def _branch_route_partition_file(
    run_root: Path,
    route_name: str,
    route_step_name: str,
    partition_key: str,
    suffix: str = ".json",
) -> Path:
    return (
        run_root
        / f"_001_{BRONZE_STAGE_NAME}"
        / f"_001_{BRANCH_STEP_NAME}"
        / route_name
        / route_step_name
        / f"{partition_key}{suffix}"
    )


def _gold_step_dir(run_root: Path) -> Path:
    return run_root / f"_003_{GOLD_STAGE_NAME}" / f"_001_{GOLD_STEP_NAME}"


def _gold_partition_keys(run_root: Path) -> set[str]:
    gold_dir = _gold_step_dir(run_root)
    return {
        file_path.relative_to(gold_dir).as_posix().removesuffix(".json")
        for file_path in gold_dir.rglob("*.json")
    }


def test_branching_pipeline_run_writes_bronze_route_subfolders(tmp_path: Path) -> None:
    pipeline = build_branching_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    json_route_file = _branch_route_partition_file(
        run_root,
        JSON_SOURCE_NAME,
        BRONZE_ROUTE_JSON_STEP_NAME,
        "json-alpha",
    )
    html_route_file = _branch_route_partition_file(
        run_root,
        HTML_SOURCE_NAME,
        BRONZE_ROUTE_HTML_STEP_NAME,
        "html-alpha",
    )

    assert json_route_file.exists()
    assert html_route_file.exists()


def test_branching_pipeline_silver_records_have_uniform_schema(tmp_path: Path) -> None:
    pipeline = build_branching_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    silver_records = [
        json.loads(
            _partition_file(
                run_root,
                2,
                SILVER_STAGE_NAME,
                1,
                SILVER_STEP_NAME,
                "json-alpha",
            ).read_text(encoding="utf-8")
        ),
        json.loads(
            _partition_file(
                run_root,
                2,
                SILVER_STAGE_NAME,
                1,
                SILVER_STEP_NAME,
                "html-alpha",
            ).read_text(encoding="utf-8")
        ),
    ]

    for record in silver_records:
        assert set(record.keys()) >= {"id", "language", "source", "content"}
        assert record["source"] in {JSON_SOURCE_NAME, HTML_SOURCE_NAME}


def test_branching_pipeline_gold_outputs_partitioned_by_language_path_keys(
    tmp_path: Path,
) -> None:
    pipeline = build_branching_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    assert _partition_file(
        run_root,
        3,
        GOLD_STAGE_NAME,
        1,
        GOLD_STEP_NAME,
        "en/json-alpha",
    ).exists()
    assert _partition_file(
        run_root,
        3,
        GOLD_STAGE_NAME,
        1,
        GOLD_STEP_NAME,
        "fr/html-beta",
    ).exists()


def test_branching_pipeline_route_merge_collision_raises_key_collision_error(
    tmp_path: Path,
) -> None:
    pipeline = build_branching_pipeline(
        json_source_dir=_DEFAULT_COLLISION_JSON_SOURCE_DIR,
        html_source_dir=_DEFAULT_COLLISION_HTML_SOURCE_DIR,
    )

    with pytest.raises(KeyCollisionError, match="shared-001"):
        run(pipeline, run_root=tmp_path)


def test_branching_pipeline_repeat_run_with_explicit_root_is_value_deterministic(
    tmp_path: Path,
) -> None:
    pipeline = build_branching_pipeline()

    run(pipeline, run_root=tmp_path)
    first_payload = json.loads(
        _partition_file(
            tmp_path,
            3,
            GOLD_STAGE_NAME,
            1,
            GOLD_STEP_NAME,
            "en/json-alpha",
        ).read_text(encoding="utf-8")
    )

    run(pipeline, run_root=tmp_path)
    second_payload = json.loads(
        _partition_file(
            tmp_path,
            3,
            GOLD_STAGE_NAME,
            1,
            GOLD_STEP_NAME,
            "en/json-alpha",
        ).read_text(encoding="utf-8")
    )

    assert first_payload == second_payload


def test_branching_pipeline_unmapped_route_fails_before_silver(tmp_path: Path) -> None:
    pipeline = build_branching_pipeline()

    branch_entry = pipeline.stages[0].entries[0]
    assert isinstance(branch_entry, Branch)
    del branch_entry.routes[HTML_SOURCE_NAME]

    with pytest.raises(KeyError, match=HTML_SOURCE_NAME):
        run(pipeline, run_root=tmp_path)

    silver_step_dir = tmp_path / f"_002_{SILVER_STAGE_NAME}" / f"_001_{SILVER_STEP_NAME}"
    assert not silver_step_dir.exists()


def test_branching_pipeline_gold_outputs_only_language_path_style_keys(
    tmp_path: Path,
) -> None:
    pipeline = build_branching_pipeline()

    run_root = run(pipeline, run_root=tmp_path)
    keys = _gold_partition_keys(run_root)

    assert keys
    for key in keys:
        parts = key.split("/")
        assert len(parts) == 2
        assert parts[0] in {"en", "fr"}
        assert parts[1]


def test_branching_pipeline_repeat_run_preserves_gold_key_set(tmp_path: Path) -> None:
    pipeline = build_branching_pipeline()

    run(pipeline, run_root=tmp_path)
    first_keys = _gold_partition_keys(tmp_path)

    run(pipeline, run_root=tmp_path)
    second_keys = _gold_partition_keys(tmp_path)

    assert first_keys == second_keys


def test_branching_pipeline_smoke_readme_tracks_branch_stage_step_and_route_names() -> None:
    readme_path = repo_root / "examples" / "branching" / "README.md"
    readme = readme_path.read_text(encoding="utf-8")

    for required_section in [
        "## Purpose",
        "## Fixtures",
        "## Smoke Test Flow",
        "## Expected Behaviors",
        "## Failure Signals",
    ]:
        assert required_section in readme

    for token in [
        PIPELINE_NAME,
        BRONZE_STAGE_NAME,
        SILVER_STAGE_NAME,
        GOLD_STAGE_NAME,
        BRANCH_STEP_NAME,
        BRONZE_ROUTE_JSON_STEP_NAME,
        BRONZE_ROUTE_HTML_STEP_NAME,
        SILVER_STEP_NAME,
        GOLD_STEP_NAME,
        JSON_SOURCE_NAME,
        HTML_SOURCE_NAME,
    ]:
        assert token in readme
