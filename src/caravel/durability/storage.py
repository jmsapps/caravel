"""Where a run writes.

Output location belongs to the invocation. A durable run keeps its recovery
database in a reserved directory beside that output, so generated output and
authoritative state stay separate.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

STATE_FILENAME = "state.sqlite3"
INTERNAL_DIRNAME = ".caravel"


def is_remote(location: str | Path) -> bool:
    return "://" in str(location)


def as_location(value: str | Path) -> str | Path:
    """Keep an fsspec URL as text; make a filesystem path a Path."""
    return str(value) if is_remote(value) else Path(value)


def resolve_state_dir(output: str | Path, state: str | Path | None) -> Path:
    """Return the reserved recovery directory, named exactly as given.

    ``state`` names the directory itself. Without it, the directory is
    ``.caravel`` beside the output root, which keeps one database per pipeline.
    """
    if state is not None:
        if is_remote(state):
            raise ValueError(
                f"Recovery storage must be a local filesystem path, got '{state}'. "
                "It supplies the transactions durability is built on."
            )
        reserved = Path(state)
    elif is_remote(output):
        raise ValueError(
            f"Output '{output}' is remote, so recovery storage cannot sit beside it. "
            "Pass state=<local path> to place the recovery database."
        )
    else:
        reserved = Path(output).parent / INTERNAL_DIRNAME

    reserved.mkdir(parents=True, exist_ok=True)
    return reserved


def reject_overlap(state: Path, output: str | Path) -> None:
    """Keep generated output and authoritative state in separate trees."""
    if is_remote(output):
        return
    reserved = state.resolve()
    published = Path(output).resolve()
    if reserved == published:
        raise ValueError(
            f"Recovery storage and output are the same directory ('{published}'). "
            "Writing output would overwrite authoritative state."
        )
    if reserved.is_relative_to(published):
        raise ValueError(
            f"Recovery storage '{reserved}' is inside the output directory "
            f"'{published}'. Rebuilding or cleaning output would destroy the "
            "recovery database."
        )
    if published.is_relative_to(reserved):
        raise ValueError(
            f"Output '{published}' is inside recovery storage '{reserved}'. "
            "Generated results belong outside reserved execution storage."
        )


def database_path(output: str | Path, state: str | Path | None) -> Path:
    """The recovery database for a pipeline writing to ``output``."""
    reserved = resolve_state_dir(output, state)
    reject_overlap(reserved, output)
    return reserved / STATE_FILENAME


class OutputLocation:
    """The root generated output is written under."""

    def __init__(self, root: str | Path) -> None:
        self.root = as_location(root)
        if not is_remote(self.root):
            Path(self.root).mkdir(parents=True, exist_ok=True)

    def __fspath__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return f"OutputLocation({str(self.root)!r})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, OutputLocation):
            return str(self.root) == str(other.root)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(str(self.root))


__all__ = [
    "INTERNAL_DIRNAME",
    "STATE_FILENAME",
    "OutputLocation",
    "as_location",
    "database_path",
    "is_remote",
    "reject_overlap",
    "resolve_state_dir",
]
