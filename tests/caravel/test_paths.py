from datetime import datetime, timezone
from pathlib import Path

import pytest

from caravel import paths


def test_format_stage_dir_zero_pads_index() -> None:
    assert paths.format_stage_dir(1, "bronze") == "_001_bronze"
    assert paths.format_stage_dir(12, "silver") == "_012_silver"
    assert paths.format_stage_dir(123, "gold") == "_123_gold"


def test_format_step_dir_zero_pads_index() -> None:
    assert paths.format_step_dir(1, "extract") == "_001_extract"
    assert paths.format_step_dir(10, "normalize") == "_010_normalize"


def test_resolve_step_output_dir_builds_canonical_path(tmp_path: Path) -> None:
    output_dir = paths.resolve_step_output_dir(
        run_root=tmp_path,
        stage_idx=2,
        stage_name="silver",
        step_idx=3,
        step_name="simplify",
    )

    expected = tmp_path / "_002_silver" / "_003_simplify"
    assert output_dir == expected


def test_resolve_run_root_returns_override_as_is(tmp_path: Path) -> None:
    override = tmp_path / "custom" / "run"
    assert paths.resolve_run_root("education_hub", override=override) == override


def test_resolve_run_root_uses_default_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed = datetime(2026, 4, 24, 13, 14, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(paths, "_utc_now", lambda: fixed)

    result = paths.resolve_run_root("demo_pipeline")

    assert result.parts[-3:] == (
        "data",
        "demo_pipeline",
    ) + ("2026-04-24T131415Z",)


def test_validate_partition_key_allows_nested_keys() -> None:
    paths.validate_partition_key("en/record_001")
    paths.validate_partition_key("fr/group_a/record_002")


@pytest.mark.parametrize(
    "key",
    [
        "",
        "/en/record_001",
        "en\\record_001",
        "en/../record_001",
        "../record_001",
    ],
)
def test_validate_partition_key_rejects_forbidden_inputs(key: str) -> None:
    with pytest.raises(ValueError):
        paths.validate_partition_key(key)


def test_partition_key_to_relpath_applies_suffix_to_leaf_only() -> None:
    relpath = paths.partition_key_to_relpath("en/record_001", ".json")
    assert relpath == Path("en", "record_001.json")


def test_partition_key_to_relpath_flat_key() -> None:
    relpath = paths.partition_key_to_relpath("record_001", ".txt")
    assert relpath == Path("record_001.txt")
