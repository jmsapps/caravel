import json
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[4]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from examples.partitioned.pipeline import (  # noqa: E402
    BRONZE_STAGE_NAME,
    BRONZE_STEP_NAME,
    GOLD_STAGE_NAME,
    GOLD_STEP_NAME,
    PIPELINE_NAME,
    SILVER_STAGE_NAME,
    SILVER_STEP_NAME,
    build_partitioned_pipeline,
)
from pipeline.runner import run  # noqa: E402


def _partition_output_file(
    run_root: Path,
    stage_index: int,
    stage_name: str,
    step_index: int,
    step_name: str,
    partition_key: str,
    suffix: str,
) -> Path:
    return (
        run_root
        / f"_{stage_index:03d}_{stage_name}"
        / f"_{step_index:03d}_{step_name}"
        / Path(f"{partition_key}{suffix}")
    )


def test_partitioned_pipeline_run_writes_three_stage_canonical_layout(tmp_path: Path) -> None:
    pipeline = build_partitioned_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    assert run_root == tmp_path
    assert (run_root / "_001_bronze" / "_001_bronze_render_html").exists()
    assert (run_root / "_002_silver" / "_001_silver_extract_structured").exists()
    assert (run_root / "_003_gold" / "_001_gold_partition_by_language").exists()


def test_partitioned_bronze_outputs_html_partitions_with_expected_suffix(tmp_path: Path) -> None:
    pipeline = build_partitioned_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    for key in ["alpha", "beta", "gamma"]:
        html_file = _partition_output_file(
            run_root,
            1,
            BRONZE_STAGE_NAME,
            1,
            BRONZE_STEP_NAME,
            key,
            ".html",
        )
        assert html_file.exists()
        assert "<article" in html_file.read_text(encoding="utf-8")


def test_partitioned_silver_outputs_flat_json_partitions(tmp_path: Path) -> None:
    pipeline = build_partitioned_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    for key in ["alpha", "beta", "gamma"]:
        json_file = _partition_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            1,
            SILVER_STEP_NAME,
            key,
            ".json",
        )
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        assert payload["source_key"] == key


def test_partitioned_gold_outputs_nested_language_paths_from_path_style_keys(
    tmp_path: Path,
) -> None:
    pipeline = build_partitioned_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    assert _partition_output_file(
        run_root,
        3,
        GOLD_STAGE_NAME,
        1,
        GOLD_STEP_NAME,
        "en/alpha",
        ".json",
    ).exists()
    assert _partition_output_file(
        run_root,
        3,
        GOLD_STAGE_NAME,
        1,
        GOLD_STEP_NAME,
        "fr/beta",
        ".json",
    ).exists()


def test_partitioned_pipeline_outputs_are_json_loadable_in_silver_and_gold(tmp_path: Path) -> None:
    pipeline = build_partitioned_pipeline()

    run_root = run(pipeline, run_root=tmp_path)

    files = [
        _partition_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            1,
            SILVER_STEP_NAME,
            "alpha",
            ".json",
        ),
        _partition_output_file(
            run_root,
            2,
            SILVER_STAGE_NAME,
            1,
            SILVER_STEP_NAME,
            "beta",
            ".json",
        ),
        _partition_output_file(
            run_root,
            3,
            GOLD_STAGE_NAME,
            1,
            GOLD_STEP_NAME,
            "en/gamma",
            ".json",
        ),
    ]

    for output_file in files:
        assert json.loads(output_file.read_text(encoding="utf-8")) is not None


def test_partitioned_pipeline_repeat_run_with_explicit_root_is_value_deterministic(
    tmp_path: Path,
) -> None:
    pipeline = build_partitioned_pipeline()

    run(pipeline, run_root=tmp_path)
    first_payload = json.loads(
        _partition_output_file(
            tmp_path,
            3,
            GOLD_STAGE_NAME,
            1,
            GOLD_STEP_NAME,
            "en/alpha",
            ".json",
        ).read_text(encoding="utf-8")
    )

    run(pipeline, run_root=tmp_path)
    second_payload = json.loads(
        _partition_output_file(
            tmp_path,
            3,
            GOLD_STAGE_NAME,
            1,
            GOLD_STEP_NAME,
            "en/alpha",
            ".json",
        ).read_text(encoding="utf-8")
    )

    assert first_payload == second_payload


def test_partitioned_pipeline_smoke_readme_tracks_actual_stage_and_step_names() -> None:
    readme_path = repo_root / "src" / "poc" / "examples" / "partitioned" / "README.md"
    readme = readme_path.read_text(encoding="utf-8")

    for required_section in [
        "## Purpose",
        "## Fixtures",
        "## Smoke test flow",
        "## Expected behaviors",
        "## Failure signals",
    ]:
        assert required_section in readme

    for token in [
        PIPELINE_NAME,
        BRONZE_STAGE_NAME,
        SILVER_STAGE_NAME,
        GOLD_STAGE_NAME,
        BRONZE_STEP_NAME,
        SILVER_STEP_NAME,
        GOLD_STEP_NAME,
    ]:
        assert token in readme
