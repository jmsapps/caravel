import json
import sys
from pathlib import Path

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from examples.minimal.pipeline import (
    BRONZE_STAGE_NAME,
    BRONZE_STEP_NAME,
    PIPELINE_NAME,
    SILVER_FINALIZE_STEP_NAME,
    SILVER_STAGE_NAME,
    SILVER_TRANSFORM_STEP_NAME,
    build_minimal_pipeline,
)
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


def test_minimal_pipeline_run_writes_canonical_stage_step_layout(tmp_path: Path) -> None:
    pipeline = build_minimal_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    assert run_root == tmp_path
    assert _step_output_file(run_root, 1, BRONZE_STAGE_NAME, 1, BRONZE_STEP_NAME).exists()
    assert _step_output_file(
        run_root,
        2,
        SILVER_STAGE_NAME,
        1,
        SILVER_TRANSFORM_STEP_NAME,
    ).exists()
    assert _step_output_file(
        run_root,
        2,
        SILVER_STAGE_NAME,
        2,
        SILVER_FINALIZE_STEP_NAME,
    ).exists()


def test_minimal_pipeline_final_output_contains_expected_transformed_fields(tmp_path: Path) -> None:
    pipeline = build_minimal_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    final_payload = json.loads(
        _step_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            2,
            SILVER_FINALIZE_STEP_NAME,
        ).read_text(encoding="utf-8")
    )

    assert final_payload["pipeline"] == PIPELINE_NAME
    assert final_payload["count"] == 2
    assert final_payload["ids"] == ["alpha", "beta"]
    assert final_payload["total_normalized"] == pytest.approx(4.0)
    assert "mode" not in final_payload
    assert set(final_payload["records"]) == {"alpha", "beta"}
    assert all(record["bronze"] is True for record in final_payload["records"].values())


def test_minimal_pipeline_mode_param_is_reflected_in_final_output(tmp_path: Path) -> None:
    pipeline = build_minimal_pipeline()

    run_root = run(pipeline, run_root=tmp_path, params={"mode": "smoke"})

    final_payload = json.loads(
        _step_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            2,
            SILVER_FINALIZE_STEP_NAME,
        ).read_text(encoding="utf-8")
    )

    assert final_payload["mode"] == "smoke"


def test_minimal_pipeline_step_outputs_are_json_loadable(tmp_path: Path) -> None:
    pipeline = build_minimal_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    output_files = [
        _step_output_file(run_root, 1, BRONZE_STAGE_NAME, 1, BRONZE_STEP_NAME),
        _step_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            1,
            SILVER_TRANSFORM_STEP_NAME,
        ),
        _step_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            2,
            SILVER_FINALIZE_STEP_NAME,
        ),
    ]

    for output_file in output_files:
        payload = json.loads(output_file.read_text(encoding="utf-8"))
        assert payload is not None


def test_minimal_pipeline_repeat_run_with_explicit_root_is_value_deterministic(
    tmp_path: Path,
) -> None:
    pipeline = build_minimal_pipeline()

    run(pipeline, run_root=tmp_path)
    first_payload = json.loads(
        _step_output_file(
            tmp_path,
            2,
            SILVER_STAGE_NAME,
            2,
            SILVER_FINALIZE_STEP_NAME,
        ).read_text(encoding="utf-8")
    )

    run(pipeline, run_root=tmp_path)
    second_payload = json.loads(
        _step_output_file(
            tmp_path,
            2,
            SILVER_STAGE_NAME,
            2,
            SILVER_FINALIZE_STEP_NAME,
        ).read_text(encoding="utf-8")
    )

    assert first_payload == second_payload


def test_minimal_pipeline_loader_fixture_missing_raises_file_not_found(tmp_path: Path) -> None:
    pipeline = build_minimal_pipeline(input_path=tmp_path / "does_not_exist.json")

    with pytest.raises(FileNotFoundError, match="missing file path"):
        run(pipeline, run_root=tmp_path / "run")


def test_minimal_pipeline_smoke_readme_tracks_actual_stage_step_names() -> None:
    readme_path = repo_root / "examples" / "minimal" / "README.md"
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
        BRONZE_STAGE_NAME,
        SILVER_STAGE_NAME,
        BRONZE_STEP_NAME,
        SILVER_TRANSFORM_STEP_NAME,
        SILVER_FINALIZE_STEP_NAME,
    ]:
        assert token in readme
