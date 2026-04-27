from __future__ import annotations

from typing import Callable

from .types import SOURCE_FIELD, KeyCollisionError, Loader, Partitions


class CallableLoader:
    """Loader backed by a plain callable returning partitioned records."""

    def __init__(self, name: str, fn: Callable[[], Partitions]) -> None:
        self.name = name
        self.fn = fn

    def load(self) -> Partitions:
        partitions = self.fn()
        if not isinstance(partitions, dict):
            raise TypeError(
                f"CallableLoader '{self.name}' expected callable output dict[str, Any], "
                f"got {type(partitions).__name__}."
            )

        for key in partitions:
            if not isinstance(key, str):
                raise TypeError(
                    f"CallableLoader '{self.name}' expected string partition keys, "
                    f"got key type {type(key).__name__}."
                )

        return partitions


class MultiSourceLoader:
    """Compose multiple loaders and tag each record with its source."""

    def __init__(self, loaders: list[Loader]) -> None:
        self.loaders = loaders
        self.name = "multi_source"

    def load(self) -> Partitions:
        merged: Partitions = {}
        source_by_key: dict[str, str] = {}

        for child in self.loaders:
            child_partitions = child.load()
            if not isinstance(child_partitions, dict):
                raise TypeError(
                    f"MultiSourceLoader child '{child.name}' expected dict[str, Any], "
                    f"got {type(child_partitions).__name__}."
                )

            for key, record in child_partitions.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"MultiSourceLoader child '{child.name}' produced non-string "
                        f"partition key type {type(key).__name__}."
                    )

                if key in merged:
                    existing_source = source_by_key[key]
                    raise KeyCollisionError(
                        "Partition key collision on "
                        f"'{key}' between existing_source='{existing_source}' "
                        f"and incoming_source='{child.name}'."
                    )

                if isinstance(record, dict):
                    tagged_record = dict(record)
                    tagged_record[SOURCE_FIELD] = child.name
                    merged[key] = tagged_record
                else:
                    merged[key] = {SOURCE_FIELD: child.name, "content": record}

                source_by_key[key] = child.name

        return merged


def dataset_as_loader(dataset: Loader) -> Loader:
    """Identity adapter to make dataset-as-loader usage explicit in declarations."""
    return dataset
