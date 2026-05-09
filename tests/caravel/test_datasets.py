import json
from pathlib import Path

import pytest

from caravel.datasets import (
    BytesDataset,
    JSONDataset,
    PartitionedBytesDataset,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    TextDataset,
)


def test_json_dataset_round_trip_single_file(tmp_path: Path) -> None:
    dataset = JSONDataset(name="json_single", path=tmp_path / "seed.json", indent=2)
    out_dir = tmp_path / "_001_step"

    payload = {"id": "1", "nested": {"ok": True}}
    dataset.save(payload, out_dir)

    expected_path = out_dir / "_001_step.json"
    assert expected_path.exists()

    dataset_with_path = JSONDataset(name="json_single", path=expected_path)
    assert dataset_with_path.load() == payload


def test_partitioned_json_dataset_round_trip_nested_keys(tmp_path: Path) -> None:
    dataset = PartitionedJSONDataset(name="json_parts")
    out_dir = tmp_path / "_002_step"

    payload = {
        "en/record_001": {"lang": "en", "id": "001"},
        "fr/record_001": {"lang": "fr", "id": "001"},
    }
    dataset.save(payload, out_dir)

    assert (out_dir / "en" / "record_001.json").exists()
    assert (out_dir / "fr" / "record_001.json").exists()

    loader = PartitionedJSONDataset(name="json_parts", path=out_dir)
    assert loader.load() == payload


def test_json_based_datasets_default_indent_to_two_spaces() -> None:
    assert JSONDataset().indent == 2
    assert PartitionedJSONDataset().indent == 2


def test_text_dataset_respects_suffix_and_encoding(tmp_path: Path) -> None:
    dataset = TextDataset(name="txt_single", suffix=".html", encoding="utf-8")
    out_dir = tmp_path / "_003_step"

    dataset.save("<h1>hello</h1>", out_dir)
    expected_path = out_dir / "_003_step.html"
    assert expected_path.read_text(encoding="utf-8") == "<h1>hello</h1>"

    loader = TextDataset(name="txt_single", path=expected_path, suffix=".html")
    assert loader.load() == "<h1>hello</h1>"


def test_partitioned_text_dataset_round_trip_nested_keys(tmp_path: Path) -> None:
    dataset = PartitionedTextDataset(name="txt_parts", suffix=".html")
    out_dir = tmp_path / "_004_step"

    payload = {"en/page_1": "<p>EN</p>", "fr/page_1": "<p>FR</p>"}
    dataset.save(payload, out_dir)

    assert (out_dir / "en" / "page_1.html").exists()
    assert (out_dir / "fr" / "page_1.html").exists()

    loader = PartitionedTextDataset(name="txt_parts", path=out_dir, suffix=".html")
    assert loader.load() == payload


def test_bytes_dataset_round_trip(tmp_path: Path) -> None:
    dataset = BytesDataset(name="bin_single", suffix=".bin")
    out_dir = tmp_path / "_005_step"

    payload = b"binary\x00content"
    dataset.save(payload, out_dir)

    expected_path = out_dir / "_005_step.bin"
    assert expected_path.read_bytes() == payload

    loader = BytesDataset(name="bin_single", path=expected_path, suffix=".bin")
    assert loader.load() == payload


def test_partitioned_bytes_dataset_round_trip_nested_keys(tmp_path: Path) -> None:
    dataset = PartitionedBytesDataset(name="bin_parts", suffix=".bin")
    out_dir = tmp_path / "_006_step"

    payload = {"en/chunk_1": b"en-bytes", "fr/chunk_1": b"fr-bytes"}
    dataset.save(payload, out_dir)

    assert (out_dir / "en" / "chunk_1.bin").exists()
    assert (out_dir / "fr" / "chunk_1.bin").exists()

    loader = PartitionedBytesDataset(name="bin_parts", path=out_dir, suffix=".bin")
    assert loader.load() == payload


@pytest.mark.parametrize(
    "dataset",
    [
        PartitionedJSONDataset(name="pj"),
        PartitionedTextDataset(name="pt"),
        PartitionedBytesDataset(name="pb"),
    ],
)
def test_partitioned_save_rejects_non_dict_payload(dataset: object, tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="dict"):
        dataset.save(["not", "a", "dict"], tmp_path / "_007_step")  # type: ignore[attr-defined]


def test_text_dataset_rejects_non_str_payload(tmp_path: Path) -> None:
    dataset = TextDataset(name="txt_single")
    with pytest.raises(TypeError, match="str"):
        dataset.save({"not": "string"}, tmp_path / "_008_step")  # type: ignore[arg-type]


def test_bytes_dataset_rejects_non_bytes_payload(tmp_path: Path) -> None:
    dataset = BytesDataset(name="bin_single")
    with pytest.raises(TypeError, match="bytes"):
        dataset.save("not-bytes", tmp_path / "_009_step")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "dataset",
    [
        JSONDataset(name="json_missing", path=Path("missing.json")),
        PartitionedJSONDataset(name="json_parts_missing", path=Path("missing_dir")),
    ],
)
def test_load_raises_file_not_found_for_missing_path(dataset: object) -> None:
    with pytest.raises(FileNotFoundError, match="missing"):
        dataset.load()  # type: ignore[attr-defined]


def test_exists_single_file_uses_step_folder_leaf_name(tmp_path: Path) -> None:
    dataset = JSONDataset(name="json_exists")
    out_dir = tmp_path / "_010_normalize"

    assert dataset.exists(out_dir) is False

    out_dir.mkdir(parents=True, exist_ok=True)
    expected_file = out_dir / "_010_normalize.json"
    expected_file.write_text(json.dumps({"ok": True}), encoding="utf-8")

    assert dataset.exists(out_dir) is True


def test_exists_partitioned_false_when_no_matching_files(tmp_path: Path) -> None:
    dataset = PartitionedTextDataset(name="txt_parts", suffix=".html")
    out_dir = tmp_path / "_011_partitioned"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "note.txt").write_text("not html", encoding="utf-8")

    assert dataset.exists(out_dir) is False


def test_describe_contains_stable_minimum_keys() -> None:
    dataset = TextDataset(name="txt_desc", path=Path("seed.txt"), suffix=".txt")
    description = dataset.describe()

    assert description["dataset"] == "TextDataset"
    assert description["name"] == "txt_desc"
    assert "path" in description
    assert "suffix" in description


def test_json_dataset_round_trip_memory_filesystem() -> None:
    dataset = JSONDataset(name="json_mem")
    out_dir = "memory://caravel/datasets/json/_001_step"
    payload = {"alpha": {"value": 1}}

    dataset.save(payload, out_dir)

    loader = JSONDataset(
        name="json_mem",
        path="memory://caravel/datasets/json/_001_step/_001_step.json",
    )
    assert loader.load() == payload
    assert dataset.exists(out_dir) is True


def test_partitioned_json_dataset_round_trip_memory_filesystem() -> None:
    dataset = PartitionedJSONDataset(name="json_parts_mem")
    out_dir = "memory://caravel/datasets/parts_json/_001_step"
    payload = {"en/alpha": {"value": 1}, "fr/beta": {"value": 2}}

    dataset.save(payload, out_dir)

    loader = PartitionedJSONDataset(
        name="json_parts_mem",
        path="memory://caravel/datasets/parts_json/_001_step",
    )
    assert loader.load() == payload
    assert dataset.exists(out_dir) is True


def test_text_dataset_round_trip_memory_filesystem() -> None:
    dataset = TextDataset(name="text_mem", suffix=".html")
    out_dir = "memory://caravel/datasets/text/_001_step"
    payload = "<p>hello</p>"

    dataset.save(payload, out_dir)

    loader = TextDataset(
        name="text_mem",
        path="memory://caravel/datasets/text/_001_step/_001_step.html",
        suffix=".html",
    )
    assert loader.load() == payload
    assert dataset.exists(out_dir) is True


def test_partitioned_text_dataset_round_trip_memory_filesystem() -> None:
    dataset = PartitionedTextDataset(name="text_parts_mem", suffix=".txt")
    out_dir = "memory://caravel/datasets/parts_text/_001_step"
    payload = {"en/alpha": "hello", "fr/beta": "bonjour"}

    dataset.save(payload, out_dir)

    loader = PartitionedTextDataset(
        name="text_parts_mem",
        path="memory://caravel/datasets/parts_text/_001_step",
        suffix=".txt",
    )
    assert loader.load() == payload
    assert dataset.exists(out_dir) is True


def test_bytes_dataset_round_trip_memory_filesystem() -> None:
    dataset = BytesDataset(name="bytes_mem", suffix=".bin")
    out_dir = "memory://caravel/datasets/bytes/_001_step"
    payload = b"binary-data"

    dataset.save(payload, out_dir)

    loader = BytesDataset(
        name="bytes_mem",
        path="memory://caravel/datasets/bytes/_001_step/_001_step.bin",
        suffix=".bin",
    )
    assert loader.load() == payload
    assert dataset.exists(out_dir) is True


def test_partitioned_bytes_dataset_round_trip_memory_filesystem() -> None:
    dataset = PartitionedBytesDataset(name="bytes_parts_mem", suffix=".bin")
    out_dir = "memory://caravel/datasets/parts_bytes/_001_step"
    payload = {"en/alpha": b"a", "fr/beta": b"b"}

    dataset.save(payload, out_dir)

    loader = PartitionedBytesDataset(
        name="bytes_parts_mem",
        path="memory://caravel/datasets/parts_bytes/_001_step",
        suffix=".bin",
    )
    assert loader.load() == payload
    assert dataset.exists(out_dir) is True
