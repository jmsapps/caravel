from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

from .paths import partition_key_to_relpath, validate_partition_key
from .storage import (
    coerce_optional_storage_path,
    ensure_parent_dir,
    ensure_storage_path_set,
    fire_failpoint,
    is_dir,
    is_file,
    iter_files_with_suffix,
    join_path,
    partitioned_output_exists,
    prepare_partitioned_save,
    relative_key_from_file,
    resolve_fs,
    single_output_path,
)
from .types import CheckpointIntegrityError, EmptyOutputError


@runtime_checkable
class CheckpointCapableDataset(Protocol):
    """Internal structural contract for centrally checkpointed datasets.

    A dataset satisfying this contract can validate a complete payload before
    any output mutation, report the partition keys a checkpoint record must
    carry, and verify physical output against a committed record. Only such
    datasets receive central checkpoint records and can serve as
    selective-rerun boundaries.
    """

    def validate_payload(self, payload: Any) -> None: ...

    def record_partition_keys(self, payload: Any) -> list[str] | None: ...

    def verify_physical_output(
        self, dest: Path | str, partition_keys: list[str] | None
    ) -> None: ...


def _validate_allow_empty(allow_empty: bool) -> None:
    if not isinstance(allow_empty, bool):
        raise TypeError(f"allow_empty must be bool, got {type(allow_empty).__name__}.")


def _reject_disallowed_empty_output(
    payload: dict[str, Any], *, allow_empty: bool, dataset_name: str
) -> None:
    if not payload and not allow_empty:
        raise EmptyOutputError(
            f"Dataset '{dataset_name}' rejected empty partitioned output; "
            "set allow_empty=True to persist it."
        )


def _validate_partitioned_payload(
    payload: Any,
    *,
    dataset_name: str,
    class_name: str,
    allow_empty: bool,
    record_type: type | None,
    record_type_label: str,
) -> None:
    """Validate a complete partitioned payload before any output is touched."""
    if not isinstance(payload, dict):
        raise TypeError(f"{class_name}.save expected dict payload, got {type(payload).__name__}.")
    _reject_disallowed_empty_output(payload, allow_empty=allow_empty, dataset_name=dataset_name)
    for key, record in payload.items():
        if not isinstance(key, str):
            raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
        validate_partition_key(key)
        if record_type is not None and not isinstance(record, record_type):
            raise TypeError(
                f"{class_name}.save expected {record_type_label} records, "
                f"got {type(record).__name__}."
            )


def _verify_single_physical_output(
    *,
    dataset_name: str,
    dest: Path | str,
    suffix: str,
    storage_options: Mapping[str, Any] | None,
    partition_keys: list[str] | None,
) -> None:
    if partition_keys is not None:
        raise CheckpointIntegrityError(
            f"Dataset '{dataset_name}' is single-file but its checkpoint record "
            "carries partition keys."
        )
    output_file = single_output_path(dest, suffix)
    fs, output_path = resolve_fs(output_file, storage_options)
    if not fs.exists(output_path):
        raise CheckpointIntegrityError(
            f"Dataset '{dataset_name}' checkpoint output missing expected file: {output_file}"
        )


def _verify_partitioned_physical_output(
    *,
    dataset_name: str,
    dest: Path | str,
    suffix: str,
    storage_options: Mapping[str, Any] | None,
    partition_keys: list[str] | None,
) -> None:
    if partition_keys is None:
        raise CheckpointIntegrityError(
            f"Dataset '{dataset_name}' is partitioned but its checkpoint record "
            "carries no partition keys."
        )
    _, dest_path = resolve_fs(dest, storage_options)
    found = sorted(
        relative_key_from_file(dest_path, file_path, suffix)
        for file_path in iter_files_with_suffix(dest, suffix, storage_options)
    )
    expected = sorted(partition_keys)
    if found != expected:
        missing = sorted(set(expected) - set(found))
        extra = sorted(set(found) - set(expected))
        raise CheckpointIntegrityError(
            f"Dataset '{dataset_name}' physical partitions do not match the checkpoint "
            f"record at '{dest}': missing={missing} extra={extra}."
        )


class JSONDataset:
    """Single-file JSON dataset."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        indent: int | None = 2,
        storage_options: Mapping[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.indent = indent
        self.storage_options = dict(storage_options) if storage_options is not None else None

    def load(self) -> Any:
        source = ensure_storage_path_set(self.name, self.path)
        fs, source_path = resolve_fs(source, self.storage_options)
        if not fs.exists(source_path) or not is_file(fs, source_path):
            raise FileNotFoundError(f"Dataset '{self.name}' missing file path: {source}")

        with fs.open(source_path, mode="rt", encoding="utf-8") as handle:
            return json.load(handle)

    def validate_payload(self, payload: Any) -> None:
        """Any JSON-serializable payload is structurally acceptable."""
        _ = payload

    def record_partition_keys(self, payload: Any) -> list[str] | None:
        _ = payload
        return None

    def verify_physical_output(self, dest: Path | str, partition_keys: list[str] | None) -> None:
        _verify_single_physical_output(
            dataset_name=self.name,
            dest=dest,
            suffix=".json",
            storage_options=self.storage_options,
            partition_keys=partition_keys,
        )

    def save(self, payload: Any, dest: Path | str) -> None:
        self.validate_payload(payload)
        output_file = single_output_path(dest, ".json")
        fs, output_path = resolve_fs(output_file, self.storage_options)
        ensure_parent_dir(fs, output_path)

        with fs.open(output_path, mode="wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=self.indent)
        fire_failpoint("after_output_file_written:1")

    def exists(self, dest: Path | str) -> bool:
        output_file = single_output_path(dest, ".json")
        fs, output_path = resolve_fs(output_file, self.storage_options)
        return bool(fs.exists(output_path))

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "indent": self.indent,
            "storage_options_configured": self.storage_options is not None,
        }


class PartitionedJSONDataset:
    """Partitioned JSON dataset (one JSON file per partition key)."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        indent: int | None = 2,
        storage_options: Mapping[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> None:
        _validate_allow_empty(allow_empty)
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.indent = indent
        self.storage_options = dict(storage_options) if storage_options is not None else None
        self.allow_empty = allow_empty

    def load(self) -> dict[str, Any]:
        source = ensure_storage_path_set(self.name, self.path)
        fs, source_path = resolve_fs(source, self.storage_options)
        if not fs.exists(source_path) or not is_dir(fs, source_path):
            raise FileNotFoundError(f"Dataset '{self.name}' missing directory path: {source}")

        loaded: dict[str, Any] = {}
        for file_path in iter_files_with_suffix(source, ".json", self.storage_options):
            key = relative_key_from_file(source_path, file_path, ".json")
            with fs.open(file_path, mode="rt", encoding="utf-8") as handle:
                loaded[key] = json.load(handle)

        return loaded

    def validate_payload(self, payload: Any) -> None:
        _validate_partitioned_payload(
            payload,
            dataset_name=self.name,
            class_name=self.__class__.__name__,
            allow_empty=self.allow_empty,
            record_type=None,
            record_type_label="JSON-serializable",
        )

    def record_partition_keys(self, payload: Any) -> list[str] | None:
        return sorted(str(key) for key in payload)

    def verify_physical_output(self, dest: Path | str, partition_keys: list[str] | None) -> None:
        _verify_partitioned_physical_output(
            dataset_name=self.name,
            dest=dest,
            suffix=".json",
            storage_options=self.storage_options,
            partition_keys=partition_keys,
        )

    def save(self, payload: Any, dest: Path | str) -> None:
        self.validate_payload(payload)

        fs, destination = resolve_fs(dest, self.storage_options)
        prepare_partitioned_save(fs, destination)

        for index, (key, record) in enumerate(payload.items(), start=1):
            relpath = partition_key_to_relpath(key, ".json").as_posix()
            output_file = join_path(destination, relpath)
            ensure_parent_dir(fs, output_file)
            with fs.open(output_file, mode="wt", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, indent=self.indent)
            fire_failpoint(f"after_output_file_written:{index}")

    def exists(self, dest: Path | str) -> bool:
        return partitioned_output_exists(dest, ".json", self.storage_options)

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "indent": self.indent,
            "allow_empty": self.allow_empty,
            "storage_options_configured": self.storage_options is not None,
        }


class TextDataset:
    """Single-file text dataset."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        suffix: str = ".txt",
        encoding: str = "utf-8",
        storage_options: Mapping[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.suffix = suffix
        self.encoding = encoding
        self.storage_options = dict(storage_options) if storage_options is not None else None

    def load(self) -> str:
        source = ensure_storage_path_set(self.name, self.path)
        fs, source_path = resolve_fs(source, self.storage_options)
        if not fs.exists(source_path) or not is_file(fs, source_path):
            raise FileNotFoundError(f"Dataset '{self.name}' missing file path: {source}")
        with fs.open(source_path, mode="rt", encoding=self.encoding) as handle:
            return str(handle.read())

    def validate_payload(self, payload: Any) -> None:
        if not isinstance(payload, str):
            raise TypeError(
                f"{self.__class__.__name__}.save expected str payload, "
                f"got {type(payload).__name__}."
            )

    def record_partition_keys(self, payload: Any) -> list[str] | None:
        _ = payload
        return None

    def verify_physical_output(self, dest: Path | str, partition_keys: list[str] | None) -> None:
        _verify_single_physical_output(
            dataset_name=self.name,
            dest=dest,
            suffix=self.suffix,
            storage_options=self.storage_options,
            partition_keys=partition_keys,
        )

    def save(self, payload: Any, dest: Path | str) -> None:
        self.validate_payload(payload)

        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        ensure_parent_dir(fs, output_path)
        with fs.open(output_path, mode="wt", encoding=self.encoding) as handle:
            handle.write(payload)
        fire_failpoint("after_output_file_written:1")

    def exists(self, dest: Path | str) -> bool:
        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        return bool(fs.exists(output_path))

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "encoding": self.encoding,
            "storage_options_configured": self.storage_options is not None,
        }


class PartitionedTextDataset:
    """Partitioned text dataset (one text file per partition key)."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        suffix: str = ".txt",
        encoding: str = "utf-8",
        storage_options: Mapping[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> None:
        _validate_allow_empty(allow_empty)
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.suffix = suffix
        self.encoding = encoding
        self.storage_options = dict(storage_options) if storage_options is not None else None
        self.allow_empty = allow_empty

    def load(self) -> dict[str, str]:
        source = ensure_storage_path_set(self.name, self.path)
        fs, source_path = resolve_fs(source, self.storage_options)
        if not fs.exists(source_path) or not is_dir(fs, source_path):
            raise FileNotFoundError(f"Dataset '{self.name}' missing directory path: {source}")

        loaded: dict[str, str] = {}
        for file_path in iter_files_with_suffix(source, self.suffix, self.storage_options):
            key = relative_key_from_file(source_path, file_path, self.suffix)
            with fs.open(file_path, mode="rt", encoding=self.encoding) as handle:
                loaded[key] = str(handle.read())

        return loaded

    def validate_payload(self, payload: Any) -> None:
        _validate_partitioned_payload(
            payload,
            dataset_name=self.name,
            class_name=self.__class__.__name__,
            allow_empty=self.allow_empty,
            record_type=str,
            record_type_label="str",
        )

    def record_partition_keys(self, payload: Any) -> list[str] | None:
        return sorted(str(key) for key in payload)

    def verify_physical_output(self, dest: Path | str, partition_keys: list[str] | None) -> None:
        _verify_partitioned_physical_output(
            dataset_name=self.name,
            dest=dest,
            suffix=self.suffix,
            storage_options=self.storage_options,
            partition_keys=partition_keys,
        )

    def save(self, payload: Any, dest: Path | str) -> None:
        self.validate_payload(payload)

        fs, destination = resolve_fs(dest, self.storage_options)
        prepare_partitioned_save(fs, destination)

        for index, (key, record) in enumerate(payload.items(), start=1):
            relpath = partition_key_to_relpath(key, self.suffix).as_posix()
            output_file = join_path(destination, relpath)
            ensure_parent_dir(fs, output_file)
            with fs.open(output_file, mode="wt", encoding=self.encoding) as handle:
                handle.write(record)
            fire_failpoint(f"after_output_file_written:{index}")

    def exists(self, dest: Path | str) -> bool:
        return partitioned_output_exists(dest, self.suffix, self.storage_options)

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "encoding": self.encoding,
            "allow_empty": self.allow_empty,
            "storage_options_configured": self.storage_options is not None,
        }


class BytesDataset:
    """Single-file binary dataset."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        suffix: str = ".bin",
        storage_options: Mapping[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.suffix = suffix
        self.storage_options = dict(storage_options) if storage_options is not None else None

    def load(self) -> bytes:
        source = ensure_storage_path_set(self.name, self.path)
        fs, source_path = resolve_fs(source, self.storage_options)
        if not fs.exists(source_path) or not is_file(fs, source_path):
            raise FileNotFoundError(f"Dataset '{self.name}' missing file path: {source}")
        with fs.open(source_path, mode="rb") as handle:
            return bytes(handle.read())

    def validate_payload(self, payload: Any) -> None:
        if not isinstance(payload, bytes):
            raise TypeError(
                f"{self.__class__.__name__}.save expected bytes payload, "
                f"got {type(payload).__name__}."
            )

    def record_partition_keys(self, payload: Any) -> list[str] | None:
        _ = payload
        return None

    def verify_physical_output(self, dest: Path | str, partition_keys: list[str] | None) -> None:
        _verify_single_physical_output(
            dataset_name=self.name,
            dest=dest,
            suffix=self.suffix,
            storage_options=self.storage_options,
            partition_keys=partition_keys,
        )

    def save(self, payload: Any, dest: Path | str) -> None:
        self.validate_payload(payload)

        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        ensure_parent_dir(fs, output_path)
        with fs.open(output_path, mode="wb") as handle:
            handle.write(payload)
        fire_failpoint("after_output_file_written:1")

    def exists(self, dest: Path | str) -> bool:
        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        return bool(fs.exists(output_path))

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "storage_options_configured": self.storage_options is not None,
        }


class PartitionedBytesDataset:
    """Partitioned binary dataset (one binary file per partition key)."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        suffix: str = ".bin",
        storage_options: Mapping[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> None:
        _validate_allow_empty(allow_empty)
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.suffix = suffix
        self.storage_options = dict(storage_options) if storage_options is not None else None
        self.allow_empty = allow_empty

    def load(self) -> dict[str, bytes]:
        source = ensure_storage_path_set(self.name, self.path)
        fs, source_path = resolve_fs(source, self.storage_options)
        if not fs.exists(source_path) or not is_dir(fs, source_path):
            raise FileNotFoundError(f"Dataset '{self.name}' missing directory path: {source}")

        loaded: dict[str, bytes] = {}
        for file_path in iter_files_with_suffix(source, self.suffix, self.storage_options):
            key = relative_key_from_file(source_path, file_path, self.suffix)
            with fs.open(file_path, mode="rb") as handle:
                loaded[key] = bytes(handle.read())

        return loaded

    def validate_payload(self, payload: Any) -> None:
        _validate_partitioned_payload(
            payload,
            dataset_name=self.name,
            class_name=self.__class__.__name__,
            allow_empty=self.allow_empty,
            record_type=bytes,
            record_type_label="bytes",
        )

    def record_partition_keys(self, payload: Any) -> list[str] | None:
        return sorted(str(key) for key in payload)

    def verify_physical_output(self, dest: Path | str, partition_keys: list[str] | None) -> None:
        _verify_partitioned_physical_output(
            dataset_name=self.name,
            dest=dest,
            suffix=self.suffix,
            storage_options=self.storage_options,
            partition_keys=partition_keys,
        )

    def save(self, payload: Any, dest: Path | str) -> None:
        self.validate_payload(payload)

        fs, destination = resolve_fs(dest, self.storage_options)
        prepare_partitioned_save(fs, destination)

        for index, (key, record) in enumerate(payload.items(), start=1):
            relpath = partition_key_to_relpath(key, self.suffix).as_posix()
            output_file = join_path(destination, relpath)
            ensure_parent_dir(fs, output_file)
            with fs.open(output_file, mode="wb") as handle:
                handle.write(record)
            fire_failpoint(f"after_output_file_written:{index}")

    def exists(self, dest: Path | str) -> bool:
        return partitioned_output_exists(dest, self.suffix, self.storage_options)

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "allow_empty": self.allow_empty,
            "storage_options_configured": self.storage_options is not None,
        }
