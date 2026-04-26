from __future__ import annotations

import re
from typing import Any, Iterable

from .branch import Branch
from .pipeline import Pipeline, Step


def _sanitize_for_id(value: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_")
    return sanitized or "node"


def _node_id(*parts: Any) -> str:
    joined = "_".join(_sanitize_for_id(str(part)) for part in parts)
    return _sanitize_for_id(joined)


def _render_node(lines: list[str], node_id: str, label: str) -> None:
    escaped = label.replace('"', '\\"')
    lines.append(f'  {node_id}["{escaped}"]')


def _render_edges(lines: list[str], from_ids: Iterable[str], to_id: str) -> None:
    for from_id in from_ids:
        lines.append(f"  {from_id} --> {to_id}")


def to_mermaid(pipeline: Pipeline) -> str:
    """Render a deterministic `flowchart TD` for a declared pipeline."""
    lines: list[str] = ["flowchart TD"]

    pipeline_id = _node_id("pipeline", pipeline.name)
    _render_node(lines, pipeline_id, f"Pipeline: {pipeline.name}")

    for stage_index, stage in enumerate(pipeline.stages, start=1):
        stage_id = _node_id("stage", stage_index, stage.name)
        _render_node(lines, stage_id, f"Stage {stage_index}: {stage.name}")
        _render_edges(lines, [pipeline_id], stage_id)

        predecessor_ids: list[str] = [stage_id]
        for entry_index, entry in enumerate(stage.entries, start=1):
            if isinstance(entry, Step):
                step_id = _node_id(
                    "stage",
                    stage_index,
                    "entry",
                    entry_index,
                    "step",
                    entry.name,
                )
                _render_node(lines, step_id, f"Step {entry_index}: {entry.name}")
                _render_edges(lines, predecessor_ids, step_id)
                predecessor_ids = [step_id]
                continue

            if isinstance(entry, Branch):
                branch_id = _node_id(
                    "stage",
                    stage_index,
                    "entry",
                    entry_index,
                    "branch",
                    entry.name,
                )
                _render_node(lines, branch_id, f"Branch {entry_index}: {entry.name}")
                _render_edges(lines, predecessor_ids, branch_id)

                route_ids: list[str] = []
                for route_index, route_key in enumerate(entry.routes, start=1):
                    route_id = _node_id(branch_id, "route", route_index, route_key)
                    _render_node(lines, route_id, f"Route: {route_key}")
                    _render_edges(lines, [branch_id], route_id)
                    route_ids.append(route_id)

                predecessor_ids = route_ids or [branch_id]
                continue

            entry_name = getattr(entry, "name", type(entry).__name__)
            generic_id = _node_id(
                "stage",
                stage_index,
                "entry",
                entry_index,
                "entry",
                entry_name,
            )
            _render_node(lines, generic_id, f"Entry {entry_index}: {entry_name}")
            _render_edges(lines, predecessor_ids, generic_id)
            predecessor_ids = [generic_id]

    return "\n".join(lines) + "\n"


__all__ = ["to_mermaid"]
