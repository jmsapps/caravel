"""Branch construct and routing helpers for branch-aware execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .types import (
    SOURCE_FIELD,
    BranchPredicate,
    KeyCollisionError,
    MissingSourceTagError,
    Partitions,
    StepFn,
)

if TYPE_CHECKING:
    from .pipeline import Step


class Branch:
    """Route partitions across source-specific (or predicate-specific) branches."""

    def __init__(
        self,
        name: str,
        by: str | BranchPredicate,
        routes: dict[str, list[StepFn | Step]],
    ) -> None:
        self.name = name
        self.by = by
        self.routes = routes

    def _route_key_for(self, partition_key: str, record: Any) -> str:
        if self.by == "source":
            route_key = self._shared_source(record)
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

    def _shared_source(self, value: Any) -> str:
        """The source tag the incoming value carries.

        A branch receives the complete value its upstream step produced, so
        ``by="source"`` routes on the source that value came from: either a
        tagged record, or a mapping of records that share one tag.
        """
        if isinstance(value, dict) and SOURCE_FIELD in value:
            tag = value[SOURCE_FIELD]
            if isinstance(tag, str):
                return tag

        if isinstance(value, dict):
            tags = {
                item[SOURCE_FIELD]
                for item in value.values()
                if isinstance(item, dict) and SOURCE_FIELD in item
            }
            if len(tags) == 1:
                return str(next(iter(tags)))
            if len(tags) > 1:
                raise MissingSourceTagError(
                    f"Branch '{self.name}' received records from "
                    f"{', '.join(sorted(str(tag) for tag in tags))}. A branch chooses "
                    "one route for the value it receives, so route a value whose "
                    "records share a source."
                )

        raise MissingSourceTagError(
            f"Branch '{self.name}' received a value carrying no '{SOURCE_FIELD}'. "
            "Tag records with their source, or declare a callable predicate."
        )

    def route_key_for(self, partition_key: str, record: Any) -> str:
        """Decide one partition's route.

        Durable execution calls this at the branch boundary and commits the
        result, so the predicate is consulted exactly once per partition even
        across restarts.
        """
        return self._route_key_for(partition_key, record)

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
