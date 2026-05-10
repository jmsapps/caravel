import json
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from caravel.runner import run
from examples.fsspec_minimal.pipeline import (
    BRONZE_STAGE_NAME,
    BRONZE_STEP_NAME,
    PIPELINE_NAME,
    SILVER_FINALIZE_STEP_NAME,
    SILVER_STAGE_NAME,
    SILVER_TRANSFORM_STEP_NAME,
    build_fsspec_pipeline,
)


def _clear_fsspec_env(monkeypatch) -> None:
    for key in [
        "CARAVEL_INPUT_URL",
        "CARAVEL_RUN_ROOT",
        "CARAVEL_BRONZE_STAGE_ROOT",
        "CARAVEL_SILVER_STAGE_ROOT",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_STORAGE_SAS_TOKEN",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ]:
        monkeypatch.delenv(key, raising=False)


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


def test_fsspec_pipeline_run_writes_canonical_stage_step_layout(
    tmp_path: Path, monkeypatch
) -> None:
    _clear_fsspec_env(monkeypatch)
    pipeline = build_fsspec_pipeline()
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


def test_fsspec_pipeline_final_output_contains_expected_summary(
    tmp_path: Path, monkeypatch
) -> None:
    _clear_fsspec_env(monkeypatch)
    pipeline = build_fsspec_pipeline()
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
    assert final_payload["ids"] == ["alpha", "beta"]
    assert final_payload["total_normalized"] == 4.0


def test_fsspec_pipeline_env_input_url_is_honored(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_fsspec_env(monkeypatch)
    fixture = repo_root / "examples" / "fsspec_minimal" / "data" / "input_partitions.json"
    monkeypatch.setenv("CARAVEL_INPUT_URL", str(fixture))

    pipeline = build_fsspec_pipeline()
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
    assert final_payload["count"] == 2
