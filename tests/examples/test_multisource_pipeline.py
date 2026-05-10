import json
import sys
from pathlib import Path

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from examples.multisource.pipeline import (
    _DEFAULT_COLLISION_SOURCE_A_DIR,
    _DEFAULT_COLLISION_SOURCE_B_DIR,
    _DEFAULT_SOURCE_A_DIR,
    _DEFAULT_SOURCE_B_DIR,
    PIPELINE_NAME,
    SOURCE_A_NAME,
    SOURCE_B_NAME,
    STAGE_NAME,
    STEP_NAME,
    build_multisource_pipeline,
    make_multisource_loader,
)
from caravel import KeyCollisionError, PartitionedJSONDataset
from caravel.runner import run


def _step_output_file(
    run_root: Path,
    stage_index: int,
    stage_name: str,
    step_index: int,
    step_name: str,
) -> Path:
    return (
        run_root
        / PIPELINE_NAME
        / f"_{stage_index:03d}_{stage_name}"
        / f"_{step_index:03d}_{step_name}"
        / f"_{step_index:03d}_{step_name}.json"
    )


def test_multisource_pipeline_run_writes_expected_summary_output(tmp_path: Path) -> None:
    pipeline = build_multisource_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    output_file = _step_output_file(run_root, 1, STAGE_NAME, 1, STEP_NAME)
    assert output_file.exists()

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert payload["pipeline"] == PIPELINE_NAME
    assert set(payload["counts_by_source"]) == {SOURCE_A_NAME, SOURCE_B_NAME}
    assert payload["total_records"] == sum(payload["counts_by_source"].values())


def test_multisource_pipeline_counts_by_source_are_non_zero_for_src_a_and_src_b(
    tmp_path: Path,
) -> None:
    pipeline = build_multisource_pipeline()

    run_root = run(pipeline, run_root=tmp_path)
    payload = json.loads(
        _step_output_file(run_root, 1, STAGE_NAME, 1, STEP_NAME).read_text(encoding="utf-8")
    )

    assert payload["counts_by_source"][SOURCE_A_NAME] > 0
    assert payload["counts_by_source"][SOURCE_B_NAME] > 0


def test_multisource_loader_collision_fixtures_raise_key_collision_error() -> None:
    loader = make_multisource_loader(
        source_a_dir=_DEFAULT_COLLISION_SOURCE_A_DIR,
        source_b_dir=_DEFAULT_COLLISION_SOURCE_B_DIR,
    )

    with pytest.raises(KeyCollisionError, match="shared_001"):
        loader.load()


def test_multisource_happy_path_fixtures_use_disjoint_partition_keys() -> None:
    source_a = PartitionedJSONDataset(name=SOURCE_A_NAME, path=_DEFAULT_SOURCE_A_DIR).load()
    source_b = PartitionedJSONDataset(name=SOURCE_B_NAME, path=_DEFAULT_SOURCE_B_DIR).load()

    assert set(source_a.keys()) == {"a_001", "a_002"}
    assert set(source_b.keys()) == {"b_001", "b_002"}
    assert set(source_a).isdisjoint(set(source_b))


def test_multisource_collision_fixtures_reuse_shared_001_key() -> None:
    collision_a = PartitionedJSONDataset(
        name=SOURCE_A_NAME,
        path=_DEFAULT_COLLISION_SOURCE_A_DIR,
    ).load()
    collision_b = PartitionedJSONDataset(
        name=SOURCE_B_NAME,
        path=_DEFAULT_COLLISION_SOURCE_B_DIR,
    ).load()

    assert set(collision_a.keys()) == {"shared_001"}
    assert set(collision_b.keys()) == {"shared_001"}


def test_multisource_pipeline_repeat_run_with_explicit_root_is_value_deterministic(
    tmp_path: Path,
) -> None:
    pipeline = build_multisource_pipeline()

    run(pipeline, run_root=tmp_path)
    first_payload = json.loads(
        _step_output_file(tmp_path, 1, STAGE_NAME, 1, STEP_NAME).read_text(encoding="utf-8")
    )

    run(pipeline, run_root=tmp_path)
    second_payload = json.loads(
        _step_output_file(tmp_path, 1, STAGE_NAME, 1, STEP_NAME).read_text(encoding="utf-8")
    )

    assert first_payload == second_payload


def test_multisource_pipeline_smoke_readme_tracks_stage_step_and_source_names() -> None:
    readme_path = repo_root / "examples" / "multisource" / "README.md"
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
        STAGE_NAME,
        STEP_NAME,
        SOURCE_A_NAME,
        SOURCE_B_NAME,
    ]:
        assert token in readme
