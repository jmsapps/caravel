from __future__ import annotations

from pathlib import Path

import fsspec

from caravel.storage import (
    coerce_optional_storage_path,
    is_url_path,
    iter_files_with_suffix,
    relative_key_from_file,
    single_output_path,
)


def test_is_url_path_detects_protocol_prefix() -> None:
    assert is_url_path("abfs://caravel/input/data.json") is True
    assert is_url_path("gs://bucket/path/file.json") is True
    assert is_url_path(Path("local/path/file.json")) is False


def test_coerce_optional_storage_path_preserves_url_strings() -> None:
    url = "abfs://caravel/input/input_partitions.json"
    coerced = coerce_optional_storage_path(url)
    assert isinstance(coerced, str)
    assert coerced == url


def test_coerce_optional_storage_path_converts_local_string_to_path() -> None:
    coerced = coerce_optional_storage_path("data/input.json")
    assert isinstance(coerced, Path)
    assert str(coerced).endswith("data/input.json")


def test_single_output_path_uses_destination_leaf_name() -> None:
    output = single_output_path(Path("runs/_001_bronze/_002_transform"), ".json")
    assert output.endswith("_002_transform/_002_transform.json")


def test_iter_files_with_suffix_lists_nested_files_for_memory_fs() -> None:
    fs, root = fsspec.core.url_to_fs("memory://caravel/test")
    fs.makedirs(root, exist_ok=True)
    with fs.open(f"{root}/en/alpha.json", "wt", encoding="utf-8") as handle:
        handle.write("{}")
    with fs.open(f"{root}/fr/beta.json", "wt", encoding="utf-8") as handle:
        handle.write("{}")
    with fs.open(f"{root}/ignore.txt", "wt", encoding="utf-8") as handle:
        handle.write("x")

    files = iter_files_with_suffix("memory://caravel/test", ".json")
    assert [path.lstrip("/") for path in files] == [
        "caravel/test/en/alpha.json",
        "caravel/test/fr/beta.json",
    ]


def test_relative_key_from_file_removes_root_and_suffix() -> None:
    key = relative_key_from_file(
        "caravel/test",
        "caravel/test/en/alpha.json",
        ".json",
    )
    assert key == "en/alpha"
