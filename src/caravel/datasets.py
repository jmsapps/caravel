from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .paths import partition_key_to_relpath, validate_partition_key
from .storage import (
    coerce_optional_storage_path,
    ensure_parent_dir,
    ensure_storage_path_set,
    is_dir,
    is_file,
    iter_files_with_suffix,
    join_path,
    relative_key_from_file,
    resolve_fs,
    single_output_path,
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

    def save(self, payload: Any, dest: Path | str) -> None:
        output_file = single_output_path(dest, ".json")
        fs, output_path = resolve_fs(output_file, self.storage_options)
        ensure_parent_dir(fs, output_path)

        with fs.open(output_path, mode="wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=self.indent)

    def exists(self, dest: Path | str) -> bool:
        output_file = single_output_path(dest, ".json")
        fs, output_path = resolve_fs(output_file, self.storage_options)
        return fs.exists(output_path)

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
    ) -> None:
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.indent = indent
        self.storage_options = dict(storage_options) if storage_options is not None else None

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

    def save(self, payload: Any, dest: Path | str) -> None:
        if not isinstance(payload, dict):
            raise TypeError(
                f"{self.__class__.__name__}.save expected dict payload, got {type(payload).__name__}."
            )

        fs, destination = resolve_fs(dest, self.storage_options)
        fs.makedirs(destination, exist_ok=True)

        for key, record in payload.items():
            if not isinstance(key, str):
                raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
            validate_partition_key(key)
            relpath = partition_key_to_relpath(key, ".json").as_posix()
            output_file = join_path(destination, relpath)
            ensure_parent_dir(fs, output_file)
            with fs.open(output_file, mode="wt", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, indent=self.indent)

    def exists(self, dest: Path | str) -> bool:
        return bool(iter_files_with_suffix(dest, ".json", self.storage_options))

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "indent": self.indent,
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

    def save(self, payload: Any, dest: Path | str) -> None:
        if not isinstance(payload, str):
            raise TypeError(
                f"{self.__class__.__name__}.save expected str payload, got {type(payload).__name__}."
            )

        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        ensure_parent_dir(fs, output_path)
        with fs.open(output_path, mode="wt", encoding=self.encoding) as handle:
            handle.write(payload)

    def exists(self, dest: Path | str) -> bool:
        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        return fs.exists(output_path)

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
    ) -> None:
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.suffix = suffix
        self.encoding = encoding
        self.storage_options = dict(storage_options) if storage_options is not None else None

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

    def save(self, payload: Any, dest: Path | str) -> None:
        if not isinstance(payload, dict):
            raise TypeError(
                f"{self.__class__.__name__}.save expected dict payload, got {type(payload).__name__}."
            )

        fs, destination = resolve_fs(dest, self.storage_options)
        fs.makedirs(destination, exist_ok=True)

        for key, record in payload.items():
            if not isinstance(key, str):
                raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
            if not isinstance(record, str):
                raise TypeError(
                    f"{self.__class__.__name__}.save expected str records, got {type(record).__name__}."
                )
            validate_partition_key(key)
            relpath = partition_key_to_relpath(key, self.suffix).as_posix()
            output_file = join_path(destination, relpath)
            ensure_parent_dir(fs, output_file)
            with fs.open(output_file, mode="wt", encoding=self.encoding) as handle:
                handle.write(record)

    def exists(self, dest: Path | str) -> bool:
        return bool(iter_files_with_suffix(dest, self.suffix, self.storage_options))

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "encoding": self.encoding,
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

    def save(self, payload: Any, dest: Path | str) -> None:
        if not isinstance(payload, bytes):
            raise TypeError(
                f"{self.__class__.__name__}.save expected bytes payload, got {type(payload).__name__}."
            )

        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        ensure_parent_dir(fs, output_path)
        with fs.open(output_path, mode="wb") as handle:
            handle.write(payload)

    def exists(self, dest: Path | str) -> bool:
        output_file = single_output_path(dest, self.suffix)
        fs, output_path = resolve_fs(output_file, self.storage_options)
        return fs.exists(output_path)

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
    ) -> None:
        self.name = name
        self.path = coerce_optional_storage_path(path)
        self.suffix = suffix
        self.storage_options = dict(storage_options) if storage_options is not None else None

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

    def save(self, payload: Any, dest: Path | str) -> None:
        if not isinstance(payload, dict):
            raise TypeError(
                f"{self.__class__.__name__}.save expected dict payload, got {type(payload).__name__}."
            )

        fs, destination = resolve_fs(dest, self.storage_options)
        fs.makedirs(destination, exist_ok=True)

        for key, record in payload.items():
            if not isinstance(key, str):
                raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
            if not isinstance(record, bytes):
                raise TypeError(
                    f"{self.__class__.__name__}.save expected bytes records, got {type(record).__name__}."
                )
            validate_partition_key(key)
            relpath = partition_key_to_relpath(key, self.suffix).as_posix()
            output_file = join_path(destination, relpath)
            ensure_parent_dir(fs, output_file)
            with fs.open(output_file, mode="wb") as handle:
                handle.write(record)

    def exists(self, dest: Path | str) -> bool:
        return bool(iter_files_with_suffix(dest, self.suffix, self.storage_options))

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "storage_options_configured": self.storage_options is not None,
        }
