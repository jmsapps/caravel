"""The pipeline's initial value.

A source loads once, producing the complete value the first Step receives.
Anything satisfying the :class:`~caravel.types.Loader` protocol is a source.
"""

from __future__ import annotations

from typing import Any, cast

from ..types import Loader


class SourceError(Exception):
    """Raised when a pipeline has no usable source."""


class InMemorySource:
    """A source holding a value already in memory."""

    def __init__(self, partitions: Any = None, *, record: Any = None, name: str = "in_memory"):
        if partitions is not None and record is not None:
            raise ValueError(
                "InMemorySource takes either partitions or record: partitions is a "
                "keyed mapping, record is one unsplit value."
            )
        self.name = name
        self._value = partitions if partitions is not None else record

    def load(self) -> Any:
        return self._value


def as_source(candidate: Any) -> Loader:
    """Return the source a pipeline loads its initial value from."""
    if candidate is None:
        raise SourceError(
            "No input source. Supply source= to run(), or declare one on the Pipeline."
        )
    if hasattr(candidate, "load"):
        return cast(Loader, candidate)
    raise TypeError(f"{type(candidate).__name__} is not a Loader: a source provides load().")


def load_initial_value(candidate: Any) -> Any:
    """Load the complete value the first Step receives."""
    return as_source(candidate).load()


__all__ = ["InMemorySource", "SourceError", "as_source", "load_initial_value"]
