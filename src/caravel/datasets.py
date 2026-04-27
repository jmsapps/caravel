from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .paths import partition_key_to_relpath, validate_partition_key


def _coerce_optional_path(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    return Path(path)


def _ensure_path_set(name: str, path: Path | None) -> Path:
    if path is None:
        raise FileNotFoundError(f"Dataset '{name}' path is not set (path=None).")
    return path


def _single_output_path(dest: Path, suffix: str) -> Path:
    return Path(dest) / f"{Path(dest).name}{suffix}"


def _iter_files_with_suffix(root: Path, suffix: str) -> list[Path]:
    return [
        candidate
        for candidate in root.rglob("*")
        if candidate.is_file() and candidate.name.endswith(suffix)
    ]


def _partition_key_from_file(root: Path, file_path: Path, suffix: str) -> str:
    rel = file_path.relative_to(root).as_posix()
    if suffix and rel.endswith(suffix):
        return rel[: -len(suffix)]
    return rel


class JSONDataset:
    """Single-file JSON dataset."""

    def __init__(
        self, name: str = "", path: str | Path | None = None, indent: int | None = 2
    ) -> None:
        self.name = name
        self.path = _coerce_optional_path(path)
        self.indent = indent

    def load(self) -> Any:
        source = _ensure_path_set(self.name, self.path)
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Dataset '{self.name}' missing file path: {source}")

        with source.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save(self, payload: Any, dest: Path) -> None:
        output_file = _single_output_path(Path(dest), ".json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with output_file.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=self.indent)

    def exists(self, dest: Path) -> bool:
        return _single_output_path(Path(dest), ".json").exists()

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "indent": self.indent,
        }


class PartitionedJSONDataset:
    """Partitioned JSON dataset (one JSON file per partition key)."""

    def __init__(
        self, name: str = "", path: str | Path | None = None, indent: int | None = 2
    ) -> None:
        self.name = name
        self.path = _coerce_optional_path(path)
        self.indent = indent

    def load(self) -> dict[str, Any]:
        source = _ensure_path_set(self.name, self.path)
        if not source.exists() or not source.is_dir():
            raise FileNotFoundError(f"Dataset '{self.name}' missing directory path: {source}")

        loaded: dict[str, Any] = {}
        for file_path in _iter_files_with_suffix(source, ".json"):
            key = _partition_key_from_file(source, file_path, ".json")
            with file_path.open("r", encoding="utf-8") as handle:
                loaded[key] = json.load(handle)

        return loaded

    def save(self, payload: Any, dest: Path) -> None:
        if not isinstance(payload, dict):
            raise TypeError(
                f"{self.__class__.__name__}.save expected dict payload, got {type(payload).__name__}."
            )

        destination = Path(dest)
        destination.mkdir(parents=True, exist_ok=True)

        for key, record in payload.items():
            if not isinstance(key, str):
                raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
            validate_partition_key(key)
            relpath = partition_key_to_relpath(key, ".json")
            output_file = destination / relpath
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with output_file.open("w", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, indent=self.indent)

    def exists(self, dest: Path) -> bool:
        destination = Path(dest)
        if not destination.exists() or not destination.is_dir():
            return False
        return any(
            file_path.name.endswith(".json")
            for file_path in destination.rglob("*")
            if file_path.is_file()
        )

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "indent": self.indent,
        }


class TextDataset:
    """Single-file text dataset."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        suffix: str = ".txt",
        encoding: str = "utf-8",
    ) -> None:
        self.name = name
        self.path = _coerce_optional_path(path)
        self.suffix = suffix
        self.encoding = encoding

    def load(self) -> str:
        source = _ensure_path_set(self.name, self.path)
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Dataset '{self.name}' missing file path: {source}")
        return source.read_text(encoding=self.encoding)

    def save(self, payload: Any, dest: Path) -> None:
        if not isinstance(payload, str):
            raise TypeError(
                f"{self.__class__.__name__}.save expected str payload, got {type(payload).__name__}."
            )

        output_file = _single_output_path(Path(dest), self.suffix)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(payload, encoding=self.encoding)

    def exists(self, dest: Path) -> bool:
        return _single_output_path(Path(dest), self.suffix).exists()

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "encoding": self.encoding,
        }


class PartitionedTextDataset:
    """Partitioned text dataset (one text file per partition key)."""

    def __init__(
        self,
        name: str = "",
        path: str | Path | None = None,
        suffix: str = ".txt",
        encoding: str = "utf-8",
    ) -> None:
        self.name = name
        self.path = _coerce_optional_path(path)
        self.suffix = suffix
        self.encoding = encoding

    def load(self) -> dict[str, str]:
        source = _ensure_path_set(self.name, self.path)
        if not source.exists() or not source.is_dir():
            raise FileNotFoundError(f"Dataset '{self.name}' missing directory path: {source}")

        loaded: dict[str, str] = {}
        for file_path in _iter_files_with_suffix(source, self.suffix):
            key = _partition_key_from_file(source, file_path, self.suffix)
            loaded[key] = file_path.read_text(encoding=self.encoding)

        return loaded

    def save(self, payload: Any, dest: Path) -> None:
        if not isinstance(payload, dict):
            raise TypeError(
                f"{self.__class__.__name__}.save expected dict payload, got {type(payload).__name__}."
            )

        destination = Path(dest)
        destination.mkdir(parents=True, exist_ok=True)

        for key, record in payload.items():
            if not isinstance(key, str):
                raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
            if not isinstance(record, str):
                raise TypeError(
                    f"{self.__class__.__name__}.save expected str records, got {type(record).__name__}."
                )
            validate_partition_key(key)
            relpath = partition_key_to_relpath(key, self.suffix)
            output_file = destination / relpath
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(record, encoding=self.encoding)

    def exists(self, dest: Path) -> bool:
        destination = Path(dest)
        if not destination.exists() or not destination.is_dir():
            return False
        return any(
            file_path.name.endswith(self.suffix)
            for file_path in destination.rglob("*")
            if file_path.is_file()
        )

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
            "encoding": self.encoding,
        }


class BytesDataset:
    """Single-file binary dataset."""

    def __init__(
        self, name: str = "", path: str | Path | None = None, suffix: str = ".bin"
    ) -> None:
        self.name = name
        self.path = _coerce_optional_path(path)
        self.suffix = suffix

    def load(self) -> bytes:
        source = _ensure_path_set(self.name, self.path)
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Dataset '{self.name}' missing file path: {source}")
        return source.read_bytes()

    def save(self, payload: Any, dest: Path) -> None:
        if not isinstance(payload, bytes):
            raise TypeError(
                f"{self.__class__.__name__}.save expected bytes payload, got {type(payload).__name__}."
            )

        output_file = _single_output_path(Path(dest), self.suffix)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(payload)

    def exists(self, dest: Path) -> bool:
        return _single_output_path(Path(dest), self.suffix).exists()

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
        }


class PartitionedBytesDataset:
    """Partitioned binary dataset (one binary file per partition key)."""

    def __init__(
        self, name: str = "", path: str | Path | None = None, suffix: str = ".bin"
    ) -> None:
        self.name = name
        self.path = _coerce_optional_path(path)
        self.suffix = suffix

    def load(self) -> dict[str, bytes]:
        source = _ensure_path_set(self.name, self.path)
        if not source.exists() or not source.is_dir():
            raise FileNotFoundError(f"Dataset '{self.name}' missing directory path: {source}")

        loaded: dict[str, bytes] = {}
        for file_path in _iter_files_with_suffix(source, self.suffix):
            key = _partition_key_from_file(source, file_path, self.suffix)
            loaded[key] = file_path.read_bytes()

        return loaded

    def save(self, payload: Any, dest: Path) -> None:
        if not isinstance(payload, dict):
            raise TypeError(
                f"{self.__class__.__name__}.save expected dict payload, got {type(payload).__name__}."
            )

        destination = Path(dest)
        destination.mkdir(parents=True, exist_ok=True)

        for key, record in payload.items():
            if not isinstance(key, str):
                raise TypeError(f"Partition key must be str, got {type(key).__name__}.")
            if not isinstance(record, bytes):
                raise TypeError(
                    f"{self.__class__.__name__}.save expected bytes records, got {type(record).__name__}."
                )
            validate_partition_key(key)
            relpath = partition_key_to_relpath(key, self.suffix)
            output_file = destination / relpath
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_bytes(record)

    def exists(self, dest: Path) -> bool:
        destination = Path(dest)
        if not destination.exists() or not destination.is_dir():
            return False
        return any(
            file_path.name.endswith(self.suffix)
            for file_path in destination.rglob("*")
            if file_path.is_file()
        )

    def describe(self) -> dict[str, Any]:
        return {
            "dataset": self.__class__.__name__,
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "suffix": self.suffix,
        }
