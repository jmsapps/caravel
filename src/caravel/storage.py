from __future__ import annotations

import posixpath
from pathlib import Path
from typing import Any, Mapping

import fsspec


StoragePath = str | Path
PARTITIONED_EMPTY_MARKER = ".caravel_empty"


def is_url_path(path: StoragePath) -> bool:
    return "://" in str(path)


def coerce_optional_storage_path(path: str | Path | None) -> StoragePath | None:
    if path is None:
        return None
    if isinstance(path, Path):
        return path
    if is_url_path(path):
        return path
    return Path(path)


def ensure_storage_path_set(name: str, path: StoragePath | None) -> StoragePath:
    if path is None:
        raise FileNotFoundError(f"Dataset '{name}' path is not set (path=None).")
    return path


def to_storage_string(path: StoragePath) -> str:
    if isinstance(path, Path):
        return path.as_posix()
    return path


def resolve_fs(
    path: StoragePath, storage_options: Mapping[str, Any] | None = None
) -> tuple[Any, str]:
    options = dict(storage_options or {})
    fs, resolved_path = fsspec.core.url_to_fs(to_storage_string(path), **options)
    return fs, str(resolved_path)


def join_path(base: StoragePath, *parts: str) -> str:
    assembled = to_storage_string(base)
    for part in parts:
        clean = part.strip("/")
        if clean:
            assembled = posixpath.join(assembled, clean)
    return assembled


def parent_path(path: StoragePath) -> str:
    normalized = to_storage_string(path).rstrip("/")
    return posixpath.dirname(normalized)


def leaf_name(path: StoragePath) -> str:
    normalized = to_storage_string(path).rstrip("/")
    return posixpath.basename(normalized)


def single_output_path(dest: StoragePath, suffix: str) -> str:
    leaf = leaf_name(dest)
    return join_path(dest, f"{leaf}{suffix}")


def ensure_parent_dir(fs: Any, path: StoragePath) -> None:
    parent = parent_path(path)
    if parent:
        fs.makedirs(parent, exist_ok=True)


def is_file(fs: Any, path: str) -> bool:
    isfile = getattr(fs, "isfile", None)
    if callable(isfile):
        try:
            return bool(isfile(path))
        except Exception:
            pass

    info = fs.info(path)
    return str(info.get("type", "")) == "file"


def iter_files_with_suffix(
    root: StoragePath,
    suffix: str,
    storage_options: Mapping[str, Any] | None = None,
) -> list[str]:
    fs, root_path = resolve_fs(root, storage_options)
    if not fs.exists(root_path):
        return []

    if is_file(fs, root_path):
        return [root_path] if root_path.endswith(suffix) else []

    try:
        candidates = list(fs.find(root_path, withdirs=False, detail=False))
    except Exception:
        pattern = join_path(root_path, "**")
        candidates = list(fs.glob(pattern))

    file_paths: list[str] = []
    for candidate in candidates:
        path = str(candidate)
        if (
            leaf_name(path) != PARTITIONED_EMPTY_MARKER
            and path.endswith(suffix)
            and is_file(fs, path)
        ):
            file_paths.append(path)

    return sorted(file_paths)


def prepare_partitioned_save(fs: Any, destination: str) -> None:
    """Create a partition destination directory."""
    fs.makedirs(destination, exist_ok=True)


def remove_and_recreate_dir(
    path: StoragePath, storage_options: Mapping[str, Any] | None = None
) -> None:
    """Replace a Caravel-managed output directory with an empty one."""
    fs, resolved = resolve_fs(path, storage_options)
    if fs.exists(resolved):
        fs.rm(resolved, recursive=True)
    fs.makedirs(resolved, exist_ok=True)


def partitioned_output_exists(
    root: StoragePath,
    suffix: str,
    storage_options: Mapping[str, Any] | None = None,
) -> bool:
    """Return whether at least one partition file exists.

    A committed-empty partitioned output is indistinguishable from absent
    output at the bare storage layer; durable empty-output evidence is a
    checkpoint-plugin concern.
    """
    return bool(iter_files_with_suffix(root, suffix, storage_options))


def relative_key_from_file(root: StoragePath, file_path: StoragePath, suffix: str) -> str:
    root_text = to_storage_string(root).rstrip("/")
    file_text = to_storage_string(file_path)

    prefix = f"{root_text}/"
    if file_text.startswith(prefix):
        rel = file_text[len(prefix) :]
    elif file_text == root_text:
        rel = ""
    else:
        rel = file_text

    if suffix and rel.endswith(suffix):
        return rel[: -len(suffix)]
    return rel


def is_dir(fs: Any, path: str) -> bool:
    isdir = getattr(fs, "isdir", None)
    if callable(isdir):
        try:
            return bool(isdir(path))
        except Exception:
            pass

    info = fs.info(path)
    return str(info.get("type", "")) == "directory"
