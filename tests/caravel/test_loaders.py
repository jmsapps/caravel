from pathlib import Path

import pytest

from caravel.datasets import PartitionedJSONDataset
from caravel.loaders import CallableLoader, MultiSourceLoader, dataset_as_loader
from caravel.types import KeyCollisionError, Partitions


class _StubLoader:
    def __init__(self, name: str, partitions: Partitions) -> None:
        self.name = name
        self._partitions = partitions

    def load(self) -> Partitions:
        return self._partitions


class _BadOutputLoader:
    def __init__(self, name: str, payload: object) -> None:
        self.name = name
        self._payload = payload

    def load(self) -> object:
        return self._payload


class _CountingDataset:
    def __init__(self) -> None:
        self.name = "counting_dataset"
        self.calls = 0

    def load(self) -> dict[str, dict[str, str]]:
        self.calls += 1
        return {"p1": {"id": "1"}}


def test_callable_loader_returns_expected_partitions() -> None:
    loader = CallableLoader(name="callable_src", fn=lambda: {"p1": {"id": "1"}})
    result = loader.load()
    assert result == {"p1": {"id": "1"}}


def test_callable_loader_rejects_non_dict_output() -> None:
    loader = CallableLoader(name="bad_callable", fn=lambda: ["not", "dict"])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bad_callable"):
        loader.load()


def test_multisource_loader_merges_disjoint_keys() -> None:
    l1 = _StubLoader("src_a", {"a": {"id": "1"}})
    l2 = _StubLoader("src_b", {"b": {"id": "2"}})

    loader = MultiSourceLoader([l1, l2])
    merged = loader.load()

    assert set(merged.keys()) == {"a", "b"}
    assert merged["a"]["__source__"] == "src_a"
    assert merged["b"]["__source__"] == "src_b"


def test_multisource_loader_collision_raises_key_collision_with_source_names() -> None:
    l1 = _StubLoader("src_a", {"same": {"id": "1"}})
    l2 = _StubLoader("src_b", {"same": {"id": "2"}})

    loader = MultiSourceLoader([l1, l2])

    with pytest.raises(KeyCollisionError, match="same") as exc:
        loader.load()

    message = str(exc.value)
    assert "src_a" in message
    assert "src_b" in message


def test_multisource_loader_injects_source_tag_for_dict_without_mutation() -> None:
    original_record = {"id": "1", "title": "Record"}
    original_partitions = {"a": original_record}

    loader = MultiSourceLoader([_StubLoader("src_a", original_partitions)])
    merged = loader.load()

    assert merged["a"]["__source__"] == "src_a"
    assert "__source__" not in original_record
    assert "__source__" not in original_partitions["a"]


def test_multisource_loader_wraps_non_dict_record_with_content() -> None:
    loader = MultiSourceLoader([_StubLoader("src_text", {"a": "hello"})])
    merged = loader.load()

    assert merged["a"] == {"__source__": "src_text", "content": "hello"}


def test_multisource_loader_rejects_non_dict_child_output() -> None:
    loader = MultiSourceLoader([_BadOutputLoader("bad_src", ["not", "dict"])])  # type: ignore[list-item]

    with pytest.raises(TypeError, match="bad_src"):
        loader.load()


def test_multisource_loader_rejects_non_string_partition_keys() -> None:
    loader = MultiSourceLoader([_StubLoader("bad_keys", {1: {"id": "1"}})])  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="bad_keys"):
        loader.load()


def test_dataset_as_loader_identity_adapter_delegates_to_dataset_load() -> None:
    dataset = _CountingDataset()

    adapted = dataset_as_loader(dataset)
    result = adapted.load()

    assert result == {"p1": {"id": "1"}}
    assert dataset.calls == 1


def test_single_source_loader_does_not_inject_source_field() -> None:
    loader = CallableLoader(name="single_src", fn=lambda: {"p1": {"id": "1"}})
    result = loader.load()

    assert "__source__" not in result["p1"]


def test_callable_loader_rejects_non_string_partition_keys() -> None:
    loader = CallableLoader(name="bad_key_callable", fn=lambda: {1: {"id": "1"}})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="bad_key_callable"):
        loader.load()


def test_dataset_as_loader_with_dataset_implementation() -> None:
    dataset = PartitionedJSONDataset(name="src_dataset", path=Path("dummy"))
    adapted = dataset_as_loader(dataset)

    assert adapted is dataset
