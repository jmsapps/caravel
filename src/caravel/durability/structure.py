"""The declared Stage and Step structure of a pipeline.

Structure is derived from the declaration alone: no processor runs, no input is
read, and nothing is hashed. That is what lets a resume rebuild the expected
structure and compare it against the rows a previous invocation stored.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from ..branch import Branch
from ..pipeline import Pipeline, Step
from ..plan import _normalize_route_step
from ..retry import RetryOptions

StepKind = Literal["step", "branch", "route", "merge"]


def _route_passthrough(value: Any, *, context: Any) -> Any:
    """Commit the incoming value unchanged.

    A branch and a merge are real Steps with real snapshots, so a route decision
    and a merge each have a committed result to restore.
    """
    return value


@dataclass(frozen=True)
class DeclaredStep:
    """One declared Step, with its identity and its place in the declaration."""

    step_id: str
    stage_id: str
    step_order: int
    name: str
    step: Step
    kind: StepKind = "step"
    branch: Branch | None = None
    branch_step_id: str | None = None
    route_key: str | None = None
    route_position: int = 0


@dataclass(frozen=True)
class DeclaredStage:
    """One declared Stage and the Steps it contains, in declaration order."""

    stage_id: str
    stage_name: str
    stage_order: int
    steps: tuple[DeclaredStep, ...]
    retry: RetryOptions | None = None


@dataclass(frozen=True)
class DeclaredStructure:
    """Every declared Stage and Step of one pipeline, in execution order."""

    stages: tuple[DeclaredStage, ...]

    def steps(self) -> tuple[DeclaredStep, ...]:
        return tuple(step for stage in self.stages for step in stage.steps)

    def step(self, step_id: str) -> DeclaredStep:
        for stage in self.stages:
            for declared in stage.steps:
                if declared.step_id == step_id:
                    return declared
        raise KeyError(step_id)

    def route_chain(self, branch_step_id: str, route_key: str) -> tuple[DeclaredStep, ...]:
        """The declared Steps of one route, in order."""
        chain = [
            step
            for step in self.steps()
            if step.branch_step_id == branch_step_id and step.route_key == route_key
        ]
        if not chain:
            raise KeyError(
                f"Route {route_key!r} is not declared on branch {branch_step_id!r}. A "
                "committed route decision names a route this pipeline still declares."
            )
        return tuple(sorted(chain, key=lambda step: step.route_position))

    def signature(self) -> tuple[tuple[str, int, str, int], ...]:
        """The identity and order this structure declares.

        A resume compares this against the stored rows; anything else about a
        Step is free to change between invocations.
        """
        return tuple(
            (stage.stage_id, stage.stage_order, step.step_id, step.step_order)
            for stage in self.stages
            for step in stage.steps
        )


def _stage_id(stage_index: int) -> str:
    return f"stage-{stage_index:03d}"


def _entry_id(stage_index: int, entry_index: int) -> str:
    return f"{_stage_id(stage_index)}-entry-{entry_index:03d}"


def _as_step(entry: Step | Callable[..., Any]) -> Step:
    return entry if isinstance(entry, Step) else Step(fn=entry)


def _branch_steps(
    branch: Branch,
    *,
    stage_id: str,
    entry_id: str,
    first_order: int,
) -> list[DeclaredStep]:
    """Expand a branch into its decision Step, its route Steps, and its merge.

    Every route a branch declares contributes its Steps, so the declared
    structure covers each path the run may take. One route executes and the
    Steps of the others are recorded as skipped.
    """
    if not branch.routes:
        raise ValueError(
            f"Branch '{branch.name}' declares no routes. A branch chooses between "
            "declared routes, so it needs at least one."
        )

    order = first_order
    steps = [
        DeclaredStep(
            step_id=entry_id,
            stage_id=stage_id,
            step_order=order,
            name=branch.name,
            step=Step(fn=_route_passthrough, name=branch.name),
            kind="branch",
            branch=branch,
        )
    ]

    for route_number, route_key in enumerate(sorted(branch.routes), start=1):
        route_entries = branch.routes[route_key]
        if not route_entries:
            raise ValueError(
                f"Branch '{branch.name}' declares route '{route_key}' with no steps. "
                "A route names the work that runs when it is chosen."
            )
        for position, entry in enumerate(route_entries, start=1):
            order += 1
            route_step = _normalize_route_step(entry, branch_name=branch.name)
            steps.append(
                DeclaredStep(
                    step_id=f"{entry_id}-route-{route_number:03d}-step-{position:03d}",
                    stage_id=stage_id,
                    step_order=order,
                    name=route_step.name or f"{branch.name}_{route_key}_{position}",
                    step=route_step,
                    kind="route",
                    branch_step_id=entry_id,
                    route_key=route_key,
                    route_position=position,
                )
            )

    order += 1
    steps.append(
        DeclaredStep(
            step_id=f"{entry_id}-merge",
            stage_id=stage_id,
            step_order=order,
            name=f"{branch.name}_merge",
            step=Step(fn=_route_passthrough, name=f"{branch.name}_merge"),
            kind="merge",
            branch_step_id=entry_id,
        )
    )
    return steps


def declare(pipeline: Pipeline) -> DeclaredStructure:
    """Build the declared structure of ``pipeline``."""
    stages: list[DeclaredStage] = []

    for stage_index, stage in enumerate(pipeline.stages, start=1):
        stage_id = _stage_id(stage_index)
        declared: list[DeclaredStep] = []
        order = 0

        for entry_index, entry in enumerate(stage.entries, start=1):
            entry_id = _entry_id(stage_index, entry_index)
            if isinstance(entry, Branch):
                declared.extend(
                    _branch_steps(
                        entry,
                        stage_id=stage_id,
                        entry_id=entry_id,
                        first_order=order + 1,
                    )
                )
                order = declared[-1].step_order
                continue

            order += 1
            step = _as_step(entry)
            declared.append(
                DeclaredStep(
                    step_id=entry_id,
                    stage_id=stage_id,
                    step_order=order,
                    name=step.name or entry_id,
                    step=step,
                )
            )

        stages.append(
            DeclaredStage(
                stage_id=stage_id,
                stage_name=stage.name,
                stage_order=stage_index,
                steps=tuple(declared),
                retry=stage.retry,
            )
        )

    return DeclaredStructure(stages=tuple(stages))


__all__ = [
    "DeclaredStage",
    "DeclaredStep",
    "DeclaredStructure",
    "StepKind",
    "declare",
]
