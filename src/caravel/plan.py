"""Logical plan compilation for Caravel pipelines.

Compilation translates a validated pipeline declaration into an immutable
:class:`LogicalPlan` with stable node IDs and explicit input references. It
performs declaration validation only: no run root, selector, storage, plugin,
or payload concern enters this layer. Runtime binding is a separate stage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Literal

from .branch import Branch
from .pipeline import Pipeline, Step, _get_decorated_output, _get_decorated_persist
from .types import Dataset

PLAN_SCHEMA_VERSION = 1

SEED_INPUT = "seed"

NodeKind = Literal["step", "fan-out", "route-step", "merge"]

_UNSAFE_NAME_SEGMENTS = ("..", ".")


def _validate_path_safe_name(value: str, *, what: str) -> None:
    """Reject identifiers that cannot form one safe directory segment."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{what} must be a non-empty string.")
    if "/" in value or "\\" in value:
        raise ValueError(f"{what} '{value}' must not contain path separators.")
    if value in _UNSAFE_NAME_SEGMENTS:
        raise ValueError(f"{what} '{value}' is not a valid path segment.")


def _normalize_route_step(step_like: Step | Callable[..., Any], *, branch_name: str) -> Step:
    """Normalize a branch route entry into a Step declaration.

    Route callables must be decorated with ``@step``; a bare callable has no
    declared output and is rejected, matching runner behavior.
    """
    if isinstance(step_like, Step):
        return step_like

    if callable(step_like):
        decorated_output = _get_decorated_output(step_like)
        if decorated_output is None:
            raise TypeError(
                f"Branch '{branch_name}' route step "
                f"'{getattr(step_like, '__name__', type(step_like).__name__)}' "
                "must be decorated with @step(output=...)."
            )
        decorated_persist = _get_decorated_persist(step_like)
        return Step(
            fn=step_like,
            output=decorated_output,
            persist=True if decorated_persist is None else decorated_persist,
        )

    raise TypeError(
        f"Branch '{branch_name}' route entry must be a Step or decorated callable, "
        f"got {type(step_like).__name__}."
    )


@dataclass(frozen=True)
class LogicalNode:
    """One node of a compiled logical plan.

    ``node_id`` is derived from stable 1-based declaration indexes only, so it
    survives renames and is path-safe. ``input_ref`` is ``"seed"``, a
    ``"node:<node_id>"`` reference, or a ``"route:<fan_out_id>:<route_key>"``
    reference to one fan-out route payload. Merge nodes carry one input
    reference per declared route in ``merge_input_refs``.
    """

    node_id: str
    kind: NodeKind
    stage_index: int
    stage_name: str
    entry_index: int
    name: str
    input_ref: str
    persist: bool = False
    dataset_type: str | None = None
    dataset_name: str | None = None
    route_key: str | None = None
    route_index: int | None = None
    route_step_index: int | None = None
    merge_input_refs: tuple[str, ...] = ()

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "kind": self.kind,
            "stage_index": self.stage_index,
            "stage_name": self.stage_name,
            "entry_index": self.entry_index,
            "name": self.name,
            "input_ref": self.input_ref,
            "persist": self.persist,
            "dataset_type": self.dataset_type,
            "dataset_name": self.dataset_name,
            "route_key": self.route_key,
            "route_index": self.route_index,
            "route_step_index": self.route_step_index,
            "merge_input_refs": list(self.merge_input_refs),
        }


@dataclass(frozen=True)
class LogicalPlan:
    """Immutable, deterministic logical execution plan for one pipeline."""

    pipeline_name: str
    nodes: tuple[LogicalNode, ...]

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "pipeline": self.pipeline_name,
            "nodes": [node.to_snapshot() for node in self.nodes],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_snapshot(), sort_keys=True, separators=(",", ":"))


def _entry_node_id(stage_index: int, entry_index: int) -> str:
    return f"stage-{stage_index:03d}-entry-{entry_index:03d}"


def _dataset_intent(dataset: Dataset) -> tuple[str, str]:
    return type(dataset).__name__, dataset.name


def _compile_step(
    entry: Step, *, stage_index: int, stage_name: str, entry_index: int, input_ref: str
) -> LogicalNode:
    dataset_type, dataset_name = _dataset_intent(entry.output)
    step_name = entry.name if entry.name is not None else ""
    _validate_path_safe_name(step_name, what=f"Step name in stage '{stage_name}'")
    return LogicalNode(
        node_id=_entry_node_id(stage_index, entry_index),
        kind="step",
        stage_index=stage_index,
        stage_name=stage_name,
        entry_index=entry_index,
        name=step_name,
        input_ref=input_ref,
        persist=entry.persist,
        dataset_type=dataset_type,
        dataset_name=dataset_name,
    )


def _compile_branch(
    entry: Branch, *, stage_index: int, stage_name: str, entry_index: int, input_ref: str
) -> list[LogicalNode]:
    _validate_path_safe_name(entry.name, what=f"Branch name in stage '{stage_name}'")
    if not (entry.by == "source" or callable(entry.by)):
        raise TypeError(
            f"Branch '{entry.name}' expected by='source' or callable, "
            f"got {type(entry.by).__name__}."
        )
    if not isinstance(entry.routes, dict) or not entry.routes:
        raise ValueError(f"Branch '{entry.name}' must declare at least one route.")

    entry_id = _entry_node_id(stage_index, entry_index)
    fan_out_id = f"{entry_id}-fanout"
    nodes: list[LogicalNode] = [
        LogicalNode(
            node_id=fan_out_id,
            kind="fan-out",
            stage_index=stage_index,
            stage_name=stage_name,
            entry_index=entry_index,
            name=entry.name,
            input_ref=input_ref,
        )
    ]

    merge_input_refs: list[str] = []
    for route_index, (route_key, route_steps) in enumerate(entry.routes.items(), start=1):
        _validate_path_safe_name(route_key, what=f"Branch '{entry.name}' route key")
        route_ref = f"route:{fan_out_id}:{route_key}"
        previous_ref = route_ref
        seen_route_step_names: set[str] = set()

        for route_step_index, route_step_like in enumerate(route_steps, start=1):
            route_step = _normalize_route_step(route_step_like, branch_name=entry.name)
            route_step_name = route_step.name if route_step.name is not None else ""
            _validate_path_safe_name(
                route_step_name,
                what=f"Branch '{entry.name}' route '{route_key}' step name",
            )
            if route_step_name in seen_route_step_names:
                raise ValueError(
                    f"Branch '{entry.name}' route '{route_key}' declares duplicate "
                    f"step name '{route_step_name}'; route output paths would collide."
                )
            seen_route_step_names.add(route_step_name)

            dataset_type, dataset_name = _dataset_intent(route_step.output)
            node_id = f"{entry_id}-route-{route_index:03d}-step-{route_step_index:03d}"
            nodes.append(
                LogicalNode(
                    node_id=node_id,
                    kind="route-step",
                    stage_index=stage_index,
                    stage_name=stage_name,
                    entry_index=entry_index,
                    name=route_step_name,
                    input_ref=previous_ref,
                    persist=route_step.persist,
                    dataset_type=dataset_type,
                    dataset_name=dataset_name,
                    route_key=route_key,
                    route_index=route_index,
                    route_step_index=route_step_index,
                )
            )
            previous_ref = f"node:{node_id}"

        merge_input_refs.append(previous_ref)

    nodes.append(
        LogicalNode(
            node_id=f"{entry_id}-merge",
            kind="merge",
            stage_index=stage_index,
            stage_name=stage_name,
            entry_index=entry_index,
            name=entry.name,
            input_ref=f"node:{fan_out_id}",
            merge_input_refs=tuple(merge_input_refs),
        )
    )
    return nodes


def compile_pipeline(pipeline: Pipeline) -> LogicalPlan:
    """Compile a pipeline declaration into a deterministic logical plan."""
    _validate_path_safe_name(pipeline.name, what="Pipeline name")

    nodes: list[LogicalNode] = []
    previous_ref = SEED_INPUT

    for stage_index, stage in enumerate(pipeline.stages, start=1):
        _validate_path_safe_name(stage.name, what="Stage name")

        for entry_index, entry in enumerate(stage.entries, start=1):
            if isinstance(entry, Step):
                node = _compile_step(
                    entry,
                    stage_index=stage_index,
                    stage_name=stage.name,
                    entry_index=entry_index,
                    input_ref=previous_ref,
                )
                nodes.append(node)
                previous_ref = f"node:{node.node_id}"
            elif isinstance(entry, Branch):
                branch_nodes = _compile_branch(
                    entry,
                    stage_index=stage_index,
                    stage_name=stage.name,
                    entry_index=entry_index,
                    input_ref=previous_ref,
                )
                nodes.extend(branch_nodes)
                previous_ref = f"node:{branch_nodes[-1].node_id}"
            else:
                raise TypeError(
                    f"Stage '{stage.name}' has unsupported entry type "
                    f"{type(entry).__name__}; expected Step or Branch."
                )

    return LogicalPlan(pipeline_name=pipeline.name, nodes=tuple(nodes))


__all__ = [
    "LogicalNode",
    "LogicalPlan",
    "NodeKind",
    "PLAN_SCHEMA_VERSION",
    "SEED_INPUT",
    "compile_pipeline",
]
