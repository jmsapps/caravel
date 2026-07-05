"""Typed plugin capabilities for extending one runner invocation.

Plugins are explicit instances passed to one runner invocation; there is no
discovery, global registry, environment activation, or event bus. A plugin
declares one or more small typed capabilities:

- **Observer** (`RunObserver`): receives immutable lifecycle events after
  committed state transitions. Observers cannot mutate plans, payloads, or
  execution results. Criticality (`required` or `best_effort`) must be
  explicit on the plugin instance.
- **Run guard** (`RunGuard`): acquires and releases an external precondition
  around one complete run. Guards enter in plugin order before user code runs
  and exit in reverse order, with teardown attempted for every guard whose
  startup completed.
- **Checkpoint policy** (`CheckpointCapability`, singleton): supplies reuse
  verdicts and durable evidence around the node commit boundary. The core
  executor owns the node state machine and invokes the capability at fixed,
  core-defined points; the capability cannot reorder, skip, or add boundary
  points, cannot invoke user code, and cannot save datasets. Skipped output
  loads only through the declared Dataset.

Stateful plugins own and require their storage configuration. The runner does
not choose a metadata location or assume plugin state lives beside pipeline
output.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

from ..types import Dataset

ObserverCriticality = Literal["required", "best_effort"]

EventKind = Literal[
    "run_started",
    "node_started",
    "node_completed",
    "node_failed",
    "node_skipped",
    "run_completed",
    "run_failed",
]


class PluginFailureError(Exception):
    """Raised when a plugin fails in a way that must fail the run.

    A required observer failure or a run-guard failure produces this distinct
    operational failure. It never invalidates checkpoint evidence that was
    already committed.
    """


@dataclass(frozen=True)
class RunFacts:
    """Immutable facts describing one run; contains no mutable runner objects."""

    pipeline_name: str
    run_id: str
    run_root: str
    is_selective: bool


@dataclass(frozen=True)
class NodeFacts:
    """Immutable facts describing one plan node."""

    node_id: str
    kind: str
    stage_index: int
    stage_name: str
    entry_index: int
    name: str
    persist: bool
    dataset_type: str | None
    dataset_name: str | None
    step_dir: str | None
    route_key: str | None


@dataclass(frozen=True)
class CheckpointContext:
    """Immutable run and node facts supplied to checkpoint callbacks."""

    run: RunFacts
    node: NodeFacts


@dataclass(frozen=True)
class CheckpointReuse:
    """A checkpoint capability's decision about one historical output.

    Boolean verdicts remain supported for compatibility. The structured form
    additionally identifies a committed-empty partitioned output, whose
    payload must be reconstructed through the declared Dataset because some
    object stores do not preserve empty directories.
    """

    reusable: bool
    committed_empty: bool = False

    def __post_init__(self) -> None:
        if self.committed_empty and not self.reusable:
            raise ValueError("A committed-empty checkpoint must be reusable.")


@dataclass(frozen=True)
class RunOutcome:
    """Terminal outcome facts handed to run guards at exit."""

    status: Literal["completed", "failed"]
    error_type: str | None = None


@dataclass(frozen=True)
class PlanOutput:
    """One bound step or route-step output declared by the current plan.

    ``managed`` is computed by core from stage configuration: it is True only
    when the output directory lives beneath the Caravel-managed pipeline root
    (no ``stage_root`` override). Ownership is a plan fact, never inferred
    from a path's name.
    """

    node_id: str
    step_dir: str
    persist: bool
    managed: bool


@dataclass(frozen=True)
class OwnershipContext:
    """Immutable bound-plan facts supplied to the ownership capability."""

    run: RunFacts
    managed_root: str
    outputs: tuple[PlanOutput, ...]


@dataclass(frozen=True)
class RunEvent:
    """One immutable lifecycle event.

    Events carry identifiers and facts only. Exception details are reduced to
    the exception class name; message sanitization is an evidence-plugin
    concern.
    """

    kind: EventKind
    run: RunFacts
    node: NodeFacts | None = None
    error_type: str | None = None


@runtime_checkable
class RunObserver(Protocol):
    """Observer capability: receives events after committed transitions."""

    def on_event(self, event: RunEvent) -> None: ...


@runtime_checkable
class RunGuard(Protocol):
    """Run-guard capability: wraps one complete run in an external precondition."""

    def enter(self, run: RunFacts) -> None: ...

    def exit(self, run: RunFacts, outcome: RunOutcome) -> None: ...


@runtime_checkable
class OwnershipCapability(Protocol):
    """Singleton ownership capability: cross-run inventory reconciliation.

    The core executor invokes ``reconcile`` once per run, after run startup
    and before any node executes, with the complete bound-plan output facts.
    Deletion candidates may come only from a previously recorded valid
    inventory and must stay inside the managed root; selective runs must
    neither prune nor replace the recorded inventory. The capability never
    invokes user code, never mutates the plan, and never creates or blesses
    checkpoint evidence.
    """

    def reconcile(self, context: OwnershipContext) -> None: ...


@runtime_checkable
class CheckpointCapability(Protocol):
    """Singleton checkpoint/reuse policy capability.

    The core executor invokes exactly this sequence for a persisted node:

        core validates payload
        -> before_replacement(...)   # invalidate prior evidence, confirm absence
        -> core replaces the owned output directory
        -> core saves through the Dataset
        -> after_save(...)           # build, publish, and verify evidence last

    and consults ``reuse_verdict`` before loading output of a node it does not
    execute. Only this capability may bless output as reusable; a verdict of
    ``False`` means the output must be recomputed. A structured
    ``CheckpointReuse`` may additionally report committed-empty output. The
    capability never calls user code, never saves datasets, and never mutates
    the plan.
    """

    def reuse_verdict(
        self, context: CheckpointContext, dataset: Dataset
    ) -> bool | CheckpointReuse: ...

    def before_replacement(self, context: CheckpointContext, dataset: Dataset) -> None: ...

    def after_save(self, context: CheckpointContext, dataset: Dataset) -> None: ...


def _validate_plugin_id(plugin_id: object) -> str:
    if not isinstance(plugin_id, str) or not plugin_id:
        raise ValueError("Plugin id must be a non-empty string.")
    if "/" in plugin_id or "\\" in plugin_id or plugin_id in ("..", "."):
        raise ValueError(f"Plugin id '{plugin_id}' must be one safe path segment.")
    return plugin_id


@dataclass(frozen=True)
class PluginSet:
    """Validated, ordered plugin composition for one runner invocation."""

    plugins: tuple[Any, ...] = ()
    observers: tuple[tuple[str, RunObserver, ObserverCriticality], ...] = ()
    guards: tuple[tuple[str, RunGuard], ...] = ()
    checkpoint: CheckpointCapability | None = None
    ownership: OwnershipCapability | None = None


def validate_plugins(plugins: tuple[Any, ...]) -> PluginSet:
    """Validate plugin ids, capability shapes, and singleton constraints."""
    seen_ids: set[str] = set()
    observers: list[tuple[str, RunObserver, ObserverCriticality]] = []
    guards: list[tuple[str, RunGuard]] = []
    checkpoint: CheckpointCapability | None = None
    ownership: OwnershipCapability | None = None

    for plugin in plugins:
        plugin_id = _validate_plugin_id(getattr(plugin, "plugin_id", None))
        if plugin_id in seen_ids:
            raise ValueError(f"Duplicate plugin id '{plugin_id}'.")
        seen_ids.add(plugin_id)

        has_capability = False

        if isinstance(plugin, RunObserver):
            criticality = getattr(plugin, "criticality", None)
            if criticality not in ("required", "best_effort"):
                raise ValueError(
                    f"Observer plugin '{plugin_id}' must declare criticality "
                    "'required' or 'best_effort' explicitly."
                )
            observers.append((plugin_id, plugin, criticality))
            has_capability = True

        if isinstance(plugin, RunGuard):
            guards.append((plugin_id, plugin))
            has_capability = True

        if isinstance(plugin, CheckpointCapability):
            if checkpoint is not None:
                raise ValueError(
                    "Only one plugin may provide the checkpoint capability; "
                    f"'{plugin_id}' conflicts with an earlier plugin."
                )
            checkpoint = plugin
            has_capability = True

        if isinstance(plugin, OwnershipCapability):
            if ownership is not None:
                raise ValueError(
                    "Only one plugin may provide the ownership capability; "
                    f"'{plugin_id}' conflicts with an earlier plugin."
                )
            ownership = plugin
            has_capability = True

        if not has_capability:
            raise ValueError(f"Plugin '{plugin_id}' declares no supported capability.")

    return PluginSet(
        plugins=tuple(plugins),
        observers=tuple(observers),
        guards=tuple(guards),
        checkpoint=checkpoint,
        ownership=ownership,
    )
