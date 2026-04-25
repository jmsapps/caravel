"""Branch construct and routing helpers for branch-aware execution."""

from __future__ import annotations

from typing import Any

from .types import (
    SOURCE_FIELD,
    BranchPredicate,
    KeyCollisionError,
    MissingSourceTagError,
    Partitions,
    StepFn,
)


class Branch:
    """Route partitions across source-specific (or predicate-specific) branches."""

    def __init__(
        self,
        name: str,
        by: str | BranchPredicate,
        routes: dict[str, list[StepFn]],
    ) -> None:
        self.name = name
        self.by = by
        self.routes = routes

    def _route_key_for(self, partition_key: str, record: Any) -> str:
        if self.by == "source":
            if not isinstance(record, dict) or SOURCE_FIELD not in record:
                raise MissingSourceTagError(
                    f"Branch '{self.name}' missing '{SOURCE_FIELD}' for partition '{partition_key}'."
                )
            route_key = record[SOURCE_FIELD]
        elif callable(self.by):
            route_key = self.by(record)
        else:
            raise TypeError(
                f"Branch '{self.name}' expected by='source' or callable, got {type(self.by).__name__}."
            )

        if not isinstance(route_key, str):
            raise TypeError(
                f"Branch '{self.name}' predicate returned non-string route key "
                f"type {type(route_key).__name__} for partition '{partition_key}'."
            )

        if route_key not in self.routes:
            raise KeyError(f"Branch '{self.name}' has no route for key '{route_key}'.")

        return route_key

    def route_partitions(self, partitions: Partitions) -> dict[str, Partitions]:
        grouped: dict[str, Partitions] = {route_key: {} for route_key in self.routes}

        for partition_key, record in partitions.items():
            route_key = self._route_key_for(partition_key, record)
            grouped[route_key][partition_key] = record

        return {
            route_key: route_partitions
            for route_key, route_partitions in grouped.items()
            if route_partitions
        }

    def merge_route_outputs(self, route_outputs: dict[str, Partitions]) -> Partitions:
        merged: Partitions = {}
        route_by_key: dict[str, str] = {}

        for route_key in self.routes:
            output_partitions = route_outputs.get(route_key, {})
            for partition_key, record in output_partitions.items():
                if partition_key in merged:
                    existing_route = route_by_key[partition_key]
                    raise KeyCollisionError(
                        "Branch output collision on "
                        f"'{partition_key}' between existing_route='{existing_route}' "
                        f"and incoming_route='{route_key}'."
                    )
                merged[partition_key] = record
                route_by_key[partition_key] = route_key

        return merged
