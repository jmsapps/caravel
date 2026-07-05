from __future__ import annotations

from pathlib import Path

from caravel.runner import run
from examples.fsspec_minimal.pipeline import build_fsspec_pipeline
from examples.production_profile import build_production_plugins


def test_production_profile_supports_full_then_selective_run(tmp_path: Path) -> None:
    run_root = tmp_path / "output"
    metadata_root = tmp_path / "metadata"
    input_path = (
        Path(__file__).parents[2] / "examples" / "fsspec_minimal" / "data" / "input_partitions.json"
    )
    pipeline = build_fsspec_pipeline(input_path=input_path)

    run(
        pipeline,
        run_root=run_root,
        plugins=build_production_plugins(metadata_root),
    )
    result = run(
        pipeline,
        run_root=run_root,
        only_stage="silver",
        plugins=build_production_plugins(metadata_root),
    )

    assert result.exists()
    assert (metadata_root / "checkpoints" / "v1" / "checkpoints").is_dir()
    assert (metadata_root / "ownership" / "v1" / "inventories").is_dir()
    assert (metadata_root / "evidence" / "v1" / "events").is_dir()
    assert not (metadata_root / "leases" / "v1" / "leases" / "fsspec_example.json").exists()
