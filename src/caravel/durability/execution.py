"""Ordered Step execution.

A pipeline runs its declared Steps in order, each over the complete value the
previous Step produced. Supplying ``durability_options`` adds the recovery
database: a committed Step is restored instead of recomputed, and commit timing
follows the declared commit scope.
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..branch import Branch
from ..logger import get_logger
from ..paths import format_stage_dir, format_step_dir
from ..pipeline import Pipeline
from ..plugins.api import (
    NodeFacts,
    PluginFailureError,
    RunEvent,
    RunFacts,
    RunOutcome,
    validate_plugins,
)
from ..retry import RetryOptions, retry_operation
from ..types import SOURCE_FIELD, StepContext
from .codecs import SnapshotCodec, SnapshotCodecs
from ..options import DurabilityOptions
from .ownership import ExecutorOwnership
from .sources import load_initial_value
from .storage import OutputLocation, database_path
from .store import RecoveryStore
from .structure import DeclaredStage, DeclaredStep, DeclaredStructure, declare
from .types import InvalidTransitionError, OutputIntegrityError

_STAGE_SAVEPOINT = "stage_attempt"

_NO_RETRY = RetryOptions(max_attempts=1)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunResult:
    """What one invocation did.

    ``steps_executed`` counts the Steps whose processors ran. ``steps_restored``
    counts the Steps whose committed snapshots were reused, so a resume reports
    the work it kept. ``outputs`` maps each persisted Step's name to the
    directory its dataset wrote, which is where a reader loads it from.
    """

    __slots__ = (
        "pipeline_name",
        "job_id",
        "output_root",
        "steps_executed",
        "steps_restored",
        "outputs",
    )

    def __init__(
        self,
        *,
        pipeline_name: str,
        job_id: str | None,
        output_root: str | Path,
        steps_executed: int,
        steps_restored: int,
        outputs: Mapping[str, str],
    ) -> None:
        self.pipeline_name = pipeline_name
        self.job_id = job_id
        self.output_root = output_root
        self.steps_executed = steps_executed
        self.steps_restored = steps_restored
        self.outputs = dict(outputs)

    def __repr__(self) -> str:
        return (
            f"RunResult(pipeline_name={self.pipeline_name!r}, job_id={self.job_id!r}, "
            f"output_root={str(self.output_root)!r}, "
            f"steps_executed={self.steps_executed}, steps_restored={self.steps_restored})"
        )


class Invocation:
    """One execution of one pipeline."""

    def __init__(
        self,
        *,
        pipeline: Pipeline,
        output: OutputLocation,
        source: Any,
        params: Mapping[str, str] | None = None,
        context_factory: Callable[[StepContext], Any] | None = None,
        keep_source_tag: bool = False,
        plugins: Sequence[Any] = (),
        state: str | Path | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.output = output
        self.source = source
        self.params = dict(params or {})
        self.context_factory = context_factory
        self.keep_source_tag = keep_source_tag
        self.logger = logger or get_logger("caravel")
        self.structure: DeclaredStructure = declare(pipeline)
        self.options: DurabilityOptions | None = pipeline.durability_options
        self.state = state

        plugin_set = validate_plugins(tuple(plugins))
        self.observers = plugin_set.observers
        self.guards = plugin_set.guards
        self.codecs = SnapshotCodecs()

        self.executed = 0
        self.restored = 0
        self._values: dict[str, Any] = {}
        self._routes: dict[str, str] = {}
        self._outputs: dict[str, str] = {}

    # -- plugin surface ----------------------------------------------------

    def _run_facts(self) -> RunFacts:
        job_id = self.options.job_id if self.options else ""
        return RunFacts(
            pipeline_name=self.pipeline.name,
            pipeline_id=self.pipeline.name,
            job_id=job_id,
            invocation_id=job_id,
            storage_root=str(self.output.root),
            restart_safe=self.options is not None,
        )

    def _node_facts(self, declared: DeclaredStep, stage: DeclaredStage) -> NodeFacts:
        dataset = declared.step.dataset
        return NodeFacts(
            node_id=declared.step_id,
            partition_id=declared.step_id,
            stage_index=stage.stage_order,
            stage_name=stage.stage_name,
            entry_index=declared.step_order,
            name=declared.name,
            dataset_type=type(dataset).__name__,
            dataset_name=dataset.name,
        )

    def _emit(self, kind: Any, **facts: Any) -> None:
        if not self.observers:
            return
        facts.setdefault("run", self._run_facts())
        event = RunEvent(kind=kind, **facts)
        for plugin_id, observer, criticality in self.observers:
            try:
                observer.on_event(event)
            except Exception as error:
                if criticality == "required":
                    raise PluginFailureError(
                        f"Required observer '{plugin_id}' failed on '{kind}': "
                        f"{type(error).__name__}."
                    ) from error
                self.logger.warning(
                    "Observer '%s' failed on '%s': %s", plugin_id, kind, type(error).__name__
                )

    # -- context and output ------------------------------------------------

    def _context(self, declared: DeclaredStep, stage: DeclaredStage) -> Any:
        base = StepContext(
            run_root=str(self.output.root),
            pipeline_name=self.pipeline.name,
            run_id=self.options.job_id if self.options else "",
            stage_index=stage.stage_order,
            stage_name=stage.stage_name,
            step_index=declared.step_order,
            step_name=declared.name,
            persist=declared.step.persist,
            step_dir=self._destination(declared, stage),
            prev_step_dir=None,
            logger=self.logger,
            params=dict(self.params),
        )
        return base if self.context_factory is None else self.context_factory(base)

    def _destination(self, declared: DeclaredStep, stage: DeclaredStage) -> str:
        root = str(self.output.root).rstrip("/")
        stage_dir = format_stage_dir(stage.stage_order, stage.stage_name)
        step_dir = format_step_dir(declared.step_order, declared.name)
        return f"{root}/{stage_dir}/{step_dir}"

    def _save(self, declared: DeclaredStep, stage: DeclaredStage, payload: Any) -> None:
        if not declared.step.persist:
            return
        dataset = declared.step.dataset
        destination = self._destination(declared, stage)
        if "://" not in destination:
            Path(destination).mkdir(parents=True, exist_ok=True)
        dataset.save(payload, destination)
        self._outputs[declared.name] = destination
        if not dataset.exists(destination):
            raise OutputIntegrityError(
                f"Step '{declared.name}' reported success, but its dataset found no "
                f"output at '{destination}'."
            )

    def _strip_source_tag(self, payload: Any) -> Any:
        if self.keep_source_tag or not isinstance(payload, dict):
            return payload
        if SOURCE_FIELD in payload:
            return {key: value for key, value in payload.items() if key != SOURCE_FIELD}
        return {
            key: self._strip_source_tag(value) if isinstance(value, dict) else value
            for key, value in payload.items()
        }

    # -- snapshots ---------------------------------------------------------

    def _codec(self, declared: DeclaredStep) -> SnapshotCodec:
        return self.codecs.for_dataset(declared.step.dataset, step_name=declared.name)

    def _encode(self, declared: DeclaredStep, payload: Any) -> tuple[bytes, str]:
        codec = self._codec(declared)
        return codec.encode(payload), codec.identity

    def _decode(self, declared: DeclaredStep, blob: bytes, identity: str) -> Any:
        self._codec(declared)
        return self.codecs.by_identity(identity, step_name=declared.name).decode(blob)

    # -- run ---------------------------------------------------------------

    def run(self) -> RunResult:
        entered: list[Any] = []
        facts = self._run_facts()
        try:
            for plugin_id, guard in self.guards:
                guard.enter(facts)
                entered.append((plugin_id, guard))
            self._emit("run_started", run=facts)
            result = self._execute()
        except BaseException as error:
            self._emit("run_failed", run=facts, error_type=type(error).__name__)
            _exit_guards(
                entered, facts, RunOutcome(status="failed", error_type=type(error).__name__)
            )
            raise
        self._emit("run_completed", run=facts)
        _exit_guards(entered, facts, RunOutcome(status="completed", error_type=None))
        return result

    def _execute(self) -> RunResult:
        if self.options is None:
            self._run_stages(store=None)
            return self._result(job_id=None)

        options = self.options
        database = database_path(self.output.root, self.state)
        ownership = ExecutorOwnership(database.parent)
        ownership.acquire(database=database)
        store = RecoveryStore(database)
        try:
            store.begin()
            store.open_run(
                job_id=options.job_id,
                pipeline_id=self.pipeline.name,
                pipeline_name=self.pipeline.name,
                commit_scope=options.commit_scope,
                structure=self.structure,
                now=_now(),
            )
            self._load_committed(store)
            try:
                self._run_stages(store=store)
            except BaseException:
                store.rollback()
                raise
            store.complete_run(job_id=options.job_id, now=_now())
            store.commit()
            return self._result(job_id=options.job_id)
        finally:
            store.close()
            ownership.release()

    def _result(self, *, job_id: str | None) -> RunResult:
        return RunResult(
            pipeline_name=self.pipeline.name,
            job_id=job_id,
            output_root=self.output.root,
            steps_executed=self.executed,
            steps_restored=self.restored,
            outputs=self._outputs,
        )

    def _load_committed(self, store: RecoveryStore) -> None:
        """Bring committed Step snapshots back into memory."""
        for step_id, record in store.steps(self._job_id).items():
            if record.route_key is not None:
                self._routes[step_id] = record.route_key
            if record.status != "completed" or record.snapshot is None:
                continue
            declared = self.structure.step(step_id)
            self._values[step_id] = self._decode(
                declared, record.snapshot, str(record.codec_identity)
            )

    @property
    def _job_id(self) -> str:
        options = self.options
        if options is None:
            raise InvalidTransitionError("This run has no durability options.")
        return options.job_id

    @property
    def _commit_scope(self) -> str:
        return self.options.commit_scope if self.options else "pipeline"

    # -- stages ------------------------------------------------------------

    def _run_stages(self, *, store: RecoveryStore | None) -> None:
        value = load_initial_value(self.source)
        for stage in self.structure.stages:
            value = self._run_stage(stage, value, store=store)

    def _run_stage(
        self, stage: DeclaredStage, incoming: Any, *, store: RecoveryStore | None
    ) -> Any:
        """Run one Stage, restarting the whole Stage when its retry says so."""

        def attempt() -> Any:
            if store is not None:
                store.savepoint(_STAGE_SAVEPOINT)
            try:
                return self._run_stage_steps(stage, incoming, store=store)
            except BaseException:
                if store is not None:
                    store.rollback_to(_STAGE_SAVEPOINT)
                self._discard_stage_values(stage, store=store)
                raise

        produced, _ = retry_operation(operation=attempt, retry=stage.retry or _NO_RETRY)

        if store is not None:
            store.complete_stage(job_id=self._job_id, stage_id=stage.stage_id, now=_now())
            if self._commit_scope in ("stage", "step"):
                store.commit()
            store.release(_STAGE_SAVEPOINT)
        return produced

    def _discard_stage_values(self, stage: DeclaredStage, *, store: RecoveryStore | None) -> None:
        """Drop in-memory results a rolled-back Stage attempt produced.

        A Step whose completion is already committed keeps its value, so the
        restart reuses it.
        """
        committed: set[str] = set()
        if store is not None:
            committed = {
                step_id
                for step_id, record in store.steps(self._job_id).items()
                if record.status == "completed"
            }
        for declared in stage.steps:
            if declared.step_id not in committed:
                self._values.pop(declared.step_id, None)

    def _run_stage_steps(
        self, stage: DeclaredStage, incoming: Any, *, store: RecoveryStore | None
    ) -> Any:
        value = incoming
        for declared in stage.steps:
            if declared.kind == "route":
                continue

            if declared.kind == "branch":
                value = self._run_step(declared, stage, value, store=store)
                route_key = self._routes[declared.step_id]
                for route_step in self.structure.route_chain(declared.step_id, route_key):
                    value = self._run_step(route_step, stage, value, store=store)
                self._skip_unchosen(declared, route_key, stage, store=store)
                continue

            value = self._run_step(declared, stage, value, store=store)
        return value

    def _skip_unchosen(
        self,
        branch_step: DeclaredStep,
        chosen: str,
        stage: DeclaredStage,
        *,
        store: RecoveryStore | None,
    ) -> None:
        """Record the Steps of the routes this run did not take."""
        if store is None:
            return
        for declared in stage.steps:
            if declared.branch_step_id != branch_step.step_id:
                continue
            if declared.route_key is None or declared.route_key == chosen:
                continue
            store.skip_step(job_id=self._job_id, step_id=declared.step_id, now=_now())

    # -- steps -------------------------------------------------------------

    def _run_step(
        self,
        declared: DeclaredStep,
        stage: DeclaredStage,
        incoming: Any,
        *,
        store: RecoveryStore | None,
    ) -> Any:
        facts = self._node_facts(declared, stage)

        if declared.step_id in self._values:
            payload = self._values[declared.step_id]
            self.restored += 1
            self._save(declared, stage, payload)
            self._emit("node_completed", node=facts)
            return payload

        self._emit("node_started", node=facts)
        context = self._context(declared, stage)
        processor = declared.step.fn

        route_key: str | None = None
        if declared.kind == "branch":
            route_key = self._decide_route(declared, incoming)

        def call() -> Any:
            return processor(copy.deepcopy(incoming), context=context)

        produced, _ = retry_operation(operation=call, retry=declared.step.retry or _NO_RETRY)
        payload = self._strip_source_tag(produced)

        self._save(declared, stage, payload)
        self._values[declared.step_id] = payload
        self.executed += 1

        if store is not None:
            blob, identity = self._encode(declared, payload)
            store.complete_step(
                job_id=self._job_id,
                step_id=declared.step_id,
                snapshot=blob,
                codec_identity=identity,
                now=_now(),
                route_key=route_key,
            )
            if self._commit_scope == "step":
                store.commit()

        self._emit("node_completed", node=facts)
        return payload

    def _decide_route(self, declared: DeclaredStep, payload: Any) -> str:
        """Choose a route once per job and keep that decision."""
        committed = self._routes.get(declared.step_id)
        if committed is not None:
            self.structure.route_chain(declared.step_id, committed)
            return committed

        branch = declared.branch
        if branch is None:
            raise InvalidTransitionError(
                f"Step '{declared.name}' is a branch without a declaration."
            )
        route_key = _route_key_for(branch, payload)
        self.structure.route_chain(declared.step_id, route_key)
        self._routes[declared.step_id] = route_key
        return route_key


def _route_key_for(branch: Branch, payload: Any) -> str:
    """Resolve the route a branch selects for the value it received."""
    return branch.route_key_for("", payload)


def _exit_guards(entered: Sequence[Any], facts: RunFacts, outcome: RunOutcome) -> None:
    failures: list[str] = []
    for plugin_id, guard in reversed(entered):
        try:
            guard.exit(facts, outcome)
        except Exception as error:
            failures.append(f"{plugin_id}: {error}")
    if failures:
        raise PluginFailureError("Run guards failed on exit: " + "; ".join(failures))


__all__ = ["Invocation", "RunResult"]
