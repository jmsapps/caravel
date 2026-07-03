import json
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from caravel.runner import run
from examples.persist_only_final.pipeline import (
    PIPELINE_NAME,
    STAGE_NAME,
    STEP_1_NAME,
    STEP_2_NAME,
    STEP_3_NAME,
    STEP_4_NAME,
    STEP_5_NAME,
    build_persist_only_final_pipeline,
)


def _step_dir(
    run_root: Path, stage_index: int, stage_name: str, step_index: int, step_name: str
) -> Path:
    return (
        run_root
        / PIPELINE_NAME
        / f"_{stage_index:03d}_{stage_name}"
        / f"_{step_index:03d}_{step_name}"
    )


def _step_file(
    run_root: Path, stage_index: int, stage_name: str, step_index: int, step_name: str
) -> Path:
    step_dir = _step_dir(run_root, stage_index, stage_name, step_index, step_name)
    return step_dir / f"_{step_index:03d}_{step_name}.json"


def test_persist_only_final_pipeline_writes_only_last_step_output(tmp_path: Path) -> None:
    pipeline = build_persist_only_final_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    assert run_root == tmp_path

    assert not _step_dir(run_root, 1, STAGE_NAME, 1, STEP_1_NAME).exists()
    assert not _step_dir(run_root, 1, STAGE_NAME, 2, STEP_2_NAME).exists()
    assert not _step_dir(run_root, 1, STAGE_NAME, 3, STEP_3_NAME).exists()
    assert not _step_dir(run_root, 1, STAGE_NAME, 4, STEP_4_NAME).exists()
    assert _step_file(run_root, 1, STAGE_NAME, 5, STEP_5_NAME).exists()


def test_persist_only_final_pipeline_final_payload_shape(tmp_path: Path) -> None:
    pipeline = build_persist_only_final_pipeline()
    run_root = run(pipeline, run_root=tmp_path)

    final_payload = json.loads(
        _step_file(run_root, 1, STAGE_NAME, 5, STEP_5_NAME).read_text(encoding="utf-8")
    )

    assert final_payload["count"] == 3
    assert final_payload["total_scaled"] == 60
    assert final_payload["ids"] == ["a", "b", "c"]
    assert set(final_payload["records"].keys()) == {"a", "b", "c"}
