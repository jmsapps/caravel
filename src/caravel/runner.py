"""Plan-driven pipeline execution.

``run`` compiles the pipeline declaration into a logical plan, binds it to one
execution request, and executes every node through a single executor seam:

    compile_pipeline(pipeline) -> LogicalPlan
    bind_execution(pipeline, logical_plan, ExecutionRequest(...)) -> ExecutionPlan
    execute(execution_plan) -> RunResult

Binding is the only layer that knows run roots and selectors. Bare core never
trusts existing output files: a selective request that must load output from a
node it does not execute fails closed at binding with
:class:`UnsupportedCapabilityError` until a checkpoint capability exists.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, NoReturn, Sequence

from .logger import get_logger
from .branch import Branch
from .datasets import CheckpointLoadableDataset, ValidatedDataset
from .paths import (
    format_stage_dir,
    format_step_dir,
    resolve_run_root,
)
from .pipeline import Pipeline, Step
from .plan import (
    SEED_INPUT,
    LogicalNode,
    LogicalPlan,
    compile_pipeline,
)
from .plan import _normalize_route_step as _plan_normalize_route_step
from .plugins import (
    CheckpointCapability,
    CheckpointContext,
    NodeFacts,
    PluginFailureError,
    PluginSet,
    RunEvent,
    RunFacts,
    RunGuard,
    RunOutcome,
    validate_plugins,
)
from .storage import is_dir, is_url_path, join_path, remove_and_recreate_dir, resolve_fs
from .types import (
    Dataset,
    MissingPriorOutputError,
    Partitions,
    StepContext,
    UnsupportedCapabilityError,
)

RunPath = Path | str


def _normalize_route_step(step_like: Step | Callable[..., Any]) -> Step:
    """Normalize one branch route entry; kept for compatibility with callers."""
    return _plan_normalize_route_step(step_like, branch_name="route")


def _is_remote_path(path: RunPath) -> bool:
    return isinstance(path, str) and is_url_path(path)


def _coerce_run_path(path: Path | str) -> RunPath:
    if isinstance(path, Path):
        return path
    if _is_remote_path(path):
        return path
    return Path(path)


def _join_run_path(base: RunPath, *parts: str) -> RunPath:
    if _is_remote_path(base):
        joined = str(base)
        for part in parts:
            joined = join_path(joined, part)
        return joined
    resolved = base if isinstance(base, Path) else Path(base)
    for part in parts:
        resolved = resolved / part
    return resolved


def _strip_source_field(payload: Any, keep_source_tag: bool) -> Any:
    from .types import SOURCE_FIELD

    if keep_source_tag:
        return payload

    if not isinstance(payload, dict):
        return payload

    if SOURCE_FIELD in payload:
        stripped = dict(payload)
        stripped.pop(SOURCE_FIELD, None)
        return stripped

    stripped_partitions: dict[str, Any] = {}
    changed = False
    for key, value in payload.items():
        if isinstance(value, dict) and SOURCE_FIELD in value:
            copied = dict(value)
            copied.pop(SOURCE_FIELD, None)
            stripped_partitions[key] = copied
            changed = True
        else:
            stripped_partitions[key] = value

    return stripped_partitions if changed else payload


def _resolve_stage_index(stages: list[Any], selector: str | int | None) -> int | None:
    if selector is None:
        return None

    if isinstance(selector, int):
        if selector < 1 or selector > len(stages):
            raise ValueError(f"Invalid stage index selector: {selector}.")
        return selector - 1

    if isinstance(selector, str):
        for idx, stage in enumerate(stages):
            if stage.name == selector:
                return idx
        raise ValueError(f"Invalid stage name selector: '{selector}'.")

    raise ValueError(f"Invalid stage selector type: {type(selector).__name__}.")


def _resolve_step_entry_index(stage: Any, selector: str | int | None) -> int | None:
    if selector is None:
        return None

    step_entries = [
        (idx, entry) for idx, entry in enumerate(stage.entries) if isinstance(entry, Step)
    ]

    if isinstance(selector, int):
        if selector < 1 or selector > len(step_entries):
            raise ValueError(f"Invalid step index selector: {selector}.")
        return step_entries[selector - 1][0]

    if isinstance(selector, str):
        for idx, entry in step_entries:
            if entry.name == selector:
                return idx
        raise ValueError(f"Invalid step name selector: '{selector}'.")

    raise ValueError(f"Invalid step selector type: {type(selector).__name__}.")


def _validate_stage_clean_policy(
    *, clean_dirs: bool, only_step_index: int | None, stage_name: str
) -> None:
    if clean_dirs and only_step_index is not None and only_step_index > 1:
        raise ValueError(
            "Stage clean_dirs=True cannot be used with selective non-first-step execution; "
            f"stage='{stage_name}' step_index={only_step_index}. "
            "Run the full stage, disable clean_dirs, or target step 1."
        )


def _clean_stage_base_if_needed(clean_root: RunPath, clean_dirs: bool) -> None:
    if not clean_dirs:
        return
    fs, root = resolve_fs(clean_root)
    if not fs.exists(root):
        fs.makedirs(root, exist_ok=True)
        return

    if not is_dir(fs, root):
        fs.rm(root)
        fs.makedirs(root, exist_ok=True)
        return

    root_prefix = root.rstrip("/")
    for child in fs.ls(root, detail=False):
        child_path = str(child["name"]) if isinstance(child, dict) else str(child)
        if root_prefix and not child_path.startswith(f"{root_prefix}/") and child_path != root:
            child_path = join_path(root, child_path)
        if child_path == root:
            continue
        fs.rm(child_path, recursive=True)

    fs.makedirs(root, exist_ok=True)


def _persist_step_output(
    dataset: Dataset,
    payload: Any,
    step_dir: RunPath,
    *,
    checkpoint: CheckpointCapability | None = None,
    checkpoint_context: CheckpointContext | None = None,
) -> None:
    """Validate the complete payload, then replace the owned step directory.

    The core executor owns this sequence; a checkpoint capability is invoked
    only at the two fixed evidence points and cannot reorder them. Datasets
    without payload validation keep plain save behavior (and receive no
    checkpoint evidence) so custom Dataset implementations are unaffected.
    """
    if not isinstance(dataset, ValidatedDataset):
        dataset.save(payload, step_dir)
        return

    dataset.validate_payload(payload)
    if checkpoint is not None and checkpoint_context is not None:
        checkpoint.before_replacement(checkpoint_context, dataset)
    remove_and_recreate_dir(step_dir, getattr(dataset, "storage_options", None))
    dataset.save(payload, step_dir)
    if checkpoint is not None and checkpoint_context is not None:
        checkpoint.after_save(checkpoint_context, dataset)


@dataclass(frozen=True)
class ExecutionRequest:
    """Per-run request facts: run root, selectors, params, and run options."""

    run_root: Path | str | None = None
    only_stage: str | int | None = None
    only_step: str | int | None = None
    params: Mapping[str, str] | None = None
    keep_source_tag: bool = False
    context_factory: Callable[[StepContext], Any] | None = None
    plugins: tuple[Any, ...] = ()


@dataclass(frozen=True)
class BoundStage:
    """One stage bound to its run-root facts."""

    index: int
    name: str
    executes: bool
    clean_dirs: bool
    clean_root: RunPath


@dataclass(frozen=True)
class BoundNode:
    """One logical node bound to its declaration objects and resolved paths."""

    logical: LogicalNode
    executes: bool
    step: Step | None = None
    branch: Branch | None = None
    step_dir: RunPath | None = None
    prev_step_dir: RunPath | None = None
    explicit_target: bool = False


@dataclass(frozen=True)
class ExecutionPlan:
    """Immutable bound plan for one runner invocation."""

    pipeline: Pipeline
    resolved_run_root: RunPath
    is_selective: bool
    keep_source_tag: bool
    params: Mapping[str, str]
    context_factory: Callable[[StepContext], Any] | None
    stages: tuple[BoundStage, ...]
    nodes: tuple[BoundNode, ...]
    plugin_set: PluginSet = PluginSet()
    capability_loads: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunResult:
    """Execution outcome facts for one run."""

    run_root: RunPath
    run_id: str
    best_effort_errors: tuple[str, ...] = ()


def bind_execution(
    pipeline: Pipeline, logical_plan: LogicalPlan, request: ExecutionRequest
) -> ExecutionPlan:
    """Bind a logical plan to one execution request.

    Binding validates request facts (selectors, clean policy, capability
    requirements) and resolves every output directory. It performs no Dataset
    I/O and never trusts existing output files.
    """
    logger = get_logger(f"caravel.runner.{pipeline.name}", log_name=f"{pipeline.name}_runner")
    is_selective = request.only_stage is not None or request.only_step is not None

    def _log_selective_failure(reason: str, **context: object) -> None:
        if not is_selective:
            return
        context_parts = " ".join(f"{k}={v!r}" for k, v in context.items())
        logger.error(
            "SELECTIVE FAILURE pipeline=%s reason=%s %s",
            pipeline.name,
            reason,
            context_parts,
        )

    if request.run_root is not None and _is_remote_path(request.run_root):
        resolved_run_root: RunPath = request.run_root
    else:
        resolved_run_root = resolve_run_root(request.run_root)

    try:
        selected_stage_idx = _resolve_stage_index(pipeline.stages, request.only_stage)
    except ValueError as exc:
        _log_selective_failure(
            "invalid-stage-selector",
            only_stage=request.only_stage,
            only_step=request.only_step,
            error=str(exc),
        )
        raise

    if request.only_step is not None and selected_stage_idx is None:
        _log_selective_failure(
            "only-step-without-stage",
            only_stage=request.only_stage,
            only_step=request.only_step,
        )
        raise ValueError("only_step requires only_stage to be set.")

    selected_step_entry_idx: int | None = None
    if selected_stage_idx is not None:
        try:
            selected_step_entry_idx = _resolve_step_entry_index(
                pipeline.stages[selected_stage_idx], request.only_step
            )
        except ValueError as exc:
            _log_selective_failure(
                "invalid-step-selector",
                only_stage=request.only_stage,
                only_step=request.only_step,
                error=str(exc),
            )
            raise

    if selected_stage_idx is not None and selected_step_entry_idx is not None:
        _validate_stage_clean_policy(
            clean_dirs=pipeline.stages[selected_stage_idx].clean_dirs,
            only_step_index=selected_step_entry_idx + 1,
            stage_name=pipeline.stages[selected_stage_idx].name,
        )

    def _resolve_stage_base(stage_index: int) -> RunPath:
        stage_decl = pipeline.stages[stage_index]
        if stage_decl.stage_root is not None:
            return _coerce_run_path(stage_decl.stage_root)
        return _join_run_path(
            resolved_run_root,
            pipeline.name,
            format_stage_dir(stage_index + 1, stage_decl.name),
        )

    bound_stages: list[BoundStage] = []
    for stage_index, stage in enumerate(pipeline.stages):
        executes = selected_stage_idx is None or selected_stage_idx == stage_index
        clean_root = _resolve_stage_base(stage_index)
        bound_stages.append(
            BoundStage(
                index=stage_index + 1,
                name=stage.name,
                executes=executes,
                clean_dirs=stage.clean_dirs,
                clean_root=clean_root,
            )
        )

    bound_nodes: list[BoundNode] = []
    prev_step_dir_by_stage: RunPath | None = None
    prev_route_dirs: dict[tuple[int, int, int], RunPath] = {}
    current_stage = 0

    for node in logical_plan.nodes:
        if node.stage_index != current_stage:
            current_stage = node.stage_index
            prev_step_dir_by_stage = None

        stage_base = _resolve_stage_base(node.stage_index - 1)
        entry = pipeline.stages[node.stage_index - 1].entries[node.entry_index - 1]

        node_executes: bool
        if selected_stage_idx is None:
            node_executes = True
        elif node.stage_index - 1 != selected_stage_idx:
            node_executes = False
        elif selected_step_entry_idx is None:
            node_executes = True
        else:
            node_executes = node.kind == "step" and node.entry_index - 1 == selected_step_entry_idx

        explicit_target = (
            node_executes and selected_step_entry_idx is not None and node.kind == "step"
        )

        if node.kind == "step":
            if not isinstance(entry, Step):
                raise TypeError(
                    f"Bound step node '{node.node_id}' does not reference a Step entry."
                )
            step_dir = _join_run_path(stage_base, format_step_dir(node.entry_index, node.name))
            bound_nodes.append(
                BoundNode(
                    logical=node,
                    executes=node_executes,
                    step=entry,
                    step_dir=step_dir,
                    prev_step_dir=prev_step_dir_by_stage,
                    explicit_target=explicit_target,
                )
            )
            prev_step_dir_by_stage = step_dir
            continue

        if not isinstance(entry, Branch):
            raise TypeError(f"Bound node '{node.node_id}' does not reference a Branch entry.")

        if node.kind == "fan-out":
            bound_nodes.append(BoundNode(logical=node, executes=node_executes, branch=entry))
            continue

        if node.kind == "route-step":
            assert node.route_index is not None and node.route_step_index is not None
            assert node.route_key is not None
            branch_dir = _join_run_path(stage_base, format_step_dir(node.entry_index, entry.name))
            step_dir = _join_run_path(branch_dir, node.route_key, node.name)
            route_chain = (node.stage_index, node.entry_index, node.route_index)
            route_step = _plan_normalize_route_step(
                entry.routes[node.route_key][node.route_step_index - 1],
                branch_name=entry.name,
            )
            bound_nodes.append(
                BoundNode(
                    logical=node,
                    executes=node_executes,
                    step=route_step,
                    branch=entry,
                    step_dir=step_dir,
                    prev_step_dir=prev_route_dirs.get(route_chain),
                )
            )
            prev_route_dirs[route_chain] = step_dir
            continue

        # merge
        bound_nodes.append(BoundNode(logical=node, executes=node_executes, branch=entry))
        prev_step_dir_by_stage = None

    nodes_by_id = {node.node_id: node for node in logical_plan.nodes}
    produced: set[str] = {SEED_INPUT}
    for bound in bound_nodes:
        if not bound.executes:
            continue
        produced.add(f"node:{bound.logical.node_id}")
        if bound.logical.kind == "fan-out" and bound.branch is not None:
            for route_key in bound.branch.routes:
                produced.add(f"route:{bound.logical.node_id}:{route_key}")

    def _describe_missing(ref: str) -> str:
        if ref.startswith("node:"):
            upstream = nodes_by_id.get(ref.removeprefix("node:"))
        elif ref.startswith("route:"):
            fan_out_id = ref.split(":", 2)[1]
            upstream = nodes_by_id.get(fan_out_id)
        else:
            upstream = None
        if upstream is None:
            return ref
        return f"stage='{upstream.stage_name}' step='{upstream.name}'"

    plugin_set = validate_plugins(request.plugins)
    bound_by_id = {bound.logical.node_id: bound for bound in bound_nodes}
    capability_loads: list[str] = []

    def _reject_unsupported(bound_node: BoundNode, ref: str, reason: str) -> NoReturn:
        _log_selective_failure(
            "checkpoint-capability-required",
            node=bound_node.logical.node_id,
            required=ref,
            cause=reason,
        )
        raise UnsupportedCapabilityError(
            "Selective execution requires committed prior output from "
            f"{_describe_missing(ref)}, but {reason}. Run the full pipeline "
            "or configure a checkpoint-capable plugin."
        )

    for bound in bound_nodes:
        if not bound.executes:
            continue
        needed = [bound.logical.input_ref, *bound.logical.merge_input_refs]
        for ref in needed:
            if ref in produced:
                continue
            if plugin_set.checkpoint is None:
                _reject_unsupported(
                    bound,
                    ref,
                    "bare Caravel has no checkpoint capability and never "
                    "trusts existing output files",
                )
            upstream_bound = (
                bound_by_id.get(ref.removeprefix("node:")) if ref.startswith("node:") else None
            )
            if upstream_bound is None or upstream_bound.logical.kind != "step":
                _reject_unsupported(
                    bound,
                    ref,
                    "the required upstream output is not a persisted step checkpoint boundary",
                )
            if not upstream_bound.logical.persist:
                _reject_unsupported(bound, ref, "the required upstream step is not persisted")
            assert upstream_bound.step is not None
            if not isinstance(upstream_bound.step.output, CheckpointLoadableDataset):
                _reject_unsupported(
                    bound,
                    ref,
                    "the required upstream Dataset does not support checkpoint loading",
                )
            node_id = upstream_bound.logical.node_id
            if node_id not in capability_loads:
                capability_loads.append(node_id)
            produced.add(ref)

    return ExecutionPlan(
        pipeline=pipeline,
        resolved_run_root=resolved_run_root,
        is_selective=is_selective,
        keep_source_tag=request.keep_source_tag,
        params=dict(request.params) if request.params is not None else {},
        context_factory=request.context_factory,
        stages=tuple(bound_stages),
        nodes=tuple(bound_nodes),
        plugin_set=plugin_set,
        capability_loads=tuple(capability_loads),
    )


def execute(execution_plan: ExecutionPlan) -> RunResult:
    """Execute every bound node through one executor dispatch.

    Plugin lifecycle: guards enter in plugin order before any user code,
    observers receive events in plugin order at committed boundaries, and
    guard teardown runs in reverse order and is attempted for every guard
    whose startup completed, even when the run fails.
    """
    pipeline = execution_plan.pipeline
    logger = get_logger(f"caravel.runner.{pipeline.name}", log_name=f"{pipeline.name}_runner")
    run_id = uuid.uuid4().hex
    resolved_run_root = execution_plan.resolved_run_root
    if isinstance(resolved_run_root, Path):
        resolved_run_root.mkdir(parents=True, exist_ok=True)

    plugin_set = execution_plan.plugin_set
    run_facts = RunFacts(
        pipeline_name=pipeline.name,
        run_id=run_id,
        run_root=str(resolved_run_root),
        is_selective=execution_plan.is_selective,
    )
    best_effort_errors: list[str] = []
    teardown_failures: list[str] = []

    def _emit(kind: Any, node: NodeFacts | None = None, error_type: str | None = None) -> None:
        event = RunEvent(kind=kind, run=run_facts, node=node, error_type=error_type)
        for plugin_id, observer, criticality in plugin_set.observers:
            try:
                observer.on_event(event)
            except Exception as exc:
                if criticality == "required":
                    raise PluginFailureError(
                        f"Required observer '{plugin_id}' failed on '{kind}': {type(exc).__name__}"
                    ) from exc
                message = (
                    f"Best-effort observer '{plugin_id}' failed on '{kind}': {type(exc).__name__}"
                )
                logger.error(message)
                best_effort_errors.append(message)

    def _node_facts(node: BoundNode) -> NodeFacts:
        logical = node.logical
        return NodeFacts(
            node_id=logical.node_id,
            kind=logical.kind,
            stage_index=logical.stage_index,
            stage_name=logical.stage_name,
            entry_index=logical.entry_index,
            name=logical.name,
            persist=logical.persist,
            dataset_type=logical.dataset_type,
            dataset_name=logical.dataset_name,
            step_dir=str(node.step_dir) if node.step_dir is not None else None,
            route_key=logical.route_key,
        )

    def _materialize_context(base_ctx: StepContext) -> Any:
        factory = execution_plan.context_factory
        if factory is None:
            return base_ctx

        runtime_context = factory(base_ctx)

        if isinstance(runtime_context, StepContext):
            return runtime_context

        runtime_context_base = getattr(runtime_context, "base", None)

        if isinstance(runtime_context_base, StepContext):
            return runtime_context

        raise TypeError(
            "context_factory must return StepContext or an object exposing a StepContext at '.base'"
        )

    executing_nodes = [node for node in execution_plan.nodes if node.executes]
    payloads: dict[str, Any] = {}

    nodes_by_id = {node.logical.node_id: node for node in execution_plan.nodes}

    def _resolve_input(node: BoundNode) -> Any:
        ref = node.logical.input_ref
        value = payloads[ref]
        if ref.startswith("node:"):
            upstream = nodes_by_id[ref.removeprefix("node:")]
            if upstream.logical.stage_index != node.logical.stage_index and not isinstance(
                value, dict
            ):
                raise TypeError(
                    f"Prior output for stage '{upstream.logical.stage_name}' step "
                    f"'{upstream.logical.name}' must be dict partitions to cross a "
                    f"stage boundary, got {type(value).__name__}."
                )
        return value

    def _load_capability_checkpoints() -> None:
        checkpoint = plugin_set.checkpoint
        if checkpoint is None:
            return
        for node_id in execution_plan.capability_loads:
            bound = nodes_by_id[node_id]
            assert bound.step is not None and bound.step_dir is not None
            facts = _node_facts(bound)
            context = CheckpointContext(run=run_facts, node=facts)
            dataset = bound.step.output
            if not checkpoint.reuse_verdict(context, dataset):
                raise MissingPriorOutputError(
                    "Checkpoint capability found no committed evidence for "
                    f"stage='{facts.stage_name}' step='{facts.name}'; the "
                    "required prior output must be recomputed."
                )
            assert isinstance(dataset, CheckpointLoadableDataset)
            payloads[f"node:{node_id}"] = dataset.load_from(bound.step_dir)
            _emit("node_skipped", node=facts)

    def _execute_step_node(node: BoundNode, step_input: Any) -> Any:
        assert node.step is not None and node.step_dir is not None
        logical = node.logical
        facts = _node_facts(node)
        checkpoint_context = CheckpointContext(run=run_facts, node=facts)
        step_index = (
            logical.route_step_index if logical.kind == "route-step" else logical.entry_index
        )

        if node.explicit_target:
            logger.info(
                "Targeted step recompute pipeline=%s stage=%s step=%s path=%s",
                pipeline.name,
                logical.stage_name,
                logical.name,
                node.step_dir,
            )

        step_ctx = StepContext(
            run_root=resolved_run_root,
            pipeline_name=pipeline.name,
            run_id=run_id,
            stage_index=logical.stage_index,
            stage_name=logical.stage_name,
            step_index=step_index if step_index is not None else logical.entry_index,
            step_name=logical.name,
            persist=logical.persist,
            step_dir=node.step_dir,
            prev_step_dir=node.prev_step_dir,
            logger=logger,
            params=dict(execution_plan.params),
        )
        runtime_context = _materialize_context(step_ctx)

        logger.info(
            "STEP START pipeline=%s stage=%s step=%s persist=%s dataset=%s",
            pipeline.name,
            logical.stage_name,
            logical.name,
            logical.persist,
            node.step.output.describe(),
        )
        _emit("node_started", node=facts)

        try:
            produced = node.step.fn(step_input, context=runtime_context)
            persisted = _strip_source_field(produced, execution_plan.keep_source_tag)
            if logical.persist:
                _persist_step_output(
                    node.step.output,
                    persisted,
                    node.step_dir,
                    checkpoint=plugin_set.checkpoint,
                    checkpoint_context=checkpoint_context,
                )
        except PluginFailureError:
            raise
        except Exception as exc:
            _emit("node_failed", node=facts, error_type=type(exc).__name__)
            raise

        logger.info(
            "STEP END pipeline=%s stage=%s step=%s persist=%s dataset=%s",
            pipeline.name,
            logical.stage_name,
            logical.name,
            logical.persist,
            node.step.output.describe(),
        )
        _emit("node_completed", node=facts)
        return persisted

    def _run_nodes() -> None:
        needs_seed = any(
            SEED_INPUT in (node.logical.input_ref, *node.logical.merge_input_refs)
            for node in executing_nodes
        )
        if needs_seed:
            payloads[SEED_INPUT] = pipeline.loader.load()

        _load_capability_checkpoints()

        current_stage_index = 0
        for node in executing_nodes:
            if node.logical.stage_index != current_stage_index:
                current_stage_index = node.logical.stage_index
                bound_stage = execution_plan.stages[current_stage_index - 1]
                _clean_stage_base_if_needed(bound_stage.clean_root, bound_stage.clean_dirs)

            logical = node.logical

            if logical.kind in ("step", "route-step"):
                step_input = _resolve_input(node)
                persisted = _execute_step_node(node, step_input)
                payloads[f"node:{logical.node_id}"] = persisted
                continue

            if logical.kind == "fan-out":
                assert node.branch is not None
                fan_input = _resolve_input(node)
                grouped = node.branch.route_partitions(fan_input)
                for route_key in node.branch.routes:
                    payloads[f"route:{logical.node_id}:{route_key}"] = grouped.get(route_key, {})
                continue

            # merge
            assert node.branch is not None
            route_outputs: dict[str, Partitions] = {}
            for route_key, ref in zip(node.branch.routes, logical.merge_input_refs):
                route_payload = payloads[ref]
                if not isinstance(route_payload, dict):
                    raise TypeError(
                        f"Branch route '{route_key}' final output must load as dict partitions."
                    )
                route_outputs[route_key] = route_payload
            payloads[f"node:{logical.node_id}"] = node.branch.merge_route_outputs(route_outputs)

    entered_guards: list[tuple[str, RunGuard]] = []
    outcome_status: Any = "completed"
    outcome_error: str | None = None
    try:
        for plugin_id, guard in plugin_set.guards:
            try:
                guard.enter(run_facts)
            except Exception as exc:
                raise PluginFailureError(
                    f"Run guard '{plugin_id}' failed to start: {type(exc).__name__}"
                ) from exc
            entered_guards.append((plugin_id, guard))

        _emit("run_started")
        _run_nodes()
        _emit("run_completed")
    except BaseException as exc:
        outcome_status = "failed"
        outcome_error = type(exc).__name__
        if not isinstance(exc, PluginFailureError):
            try:
                _emit("run_failed", error_type=type(exc).__name__)
            except PluginFailureError as observer_exc:
                logger.error("Observer failure while reporting run failure: %s", observer_exc)
        raise
    finally:
        outcome = RunOutcome(status=outcome_status, error_type=outcome_error)
        for plugin_id, guard in reversed(entered_guards):
            try:
                guard.exit(run_facts, outcome)
            except Exception as guard_exc:
                message = f"Run guard '{plugin_id}' teardown failed: {type(guard_exc).__name__}"
                logger.error(message)
                teardown_failures.append(message)

    if teardown_failures:
        raise PluginFailureError("; ".join(teardown_failures))

    return RunResult(
        run_root=resolved_run_root,
        run_id=run_id,
        best_effort_errors=tuple(best_effort_errors),
    )


def run(
    pipeline: Pipeline,
    run_root: Path | str | None = None,
    *,
    only_stage: str | int | None = None,
    only_step: str | int | None = None,
    params: Mapping[str, str] | None = None,
    keep_source_tag: bool = False,
    context_factory: Callable[[StepContext], Any] | None = None,
    plugins: Sequence[Any] | None = None,
) -> RunPath:
    """Execute a pipeline declaration and persist outputs per configured step."""
    logical_plan = compile_pipeline(pipeline)
    request = ExecutionRequest(
        run_root=run_root,
        only_stage=only_stage,
        only_step=only_step,
        params=params,
        keep_source_tag=keep_source_tag,
        context_factory=context_factory,
        plugins=tuple(plugins) if plugins is not None else (),
    )
    execution_plan = bind_execution(pipeline, logical_plan, request)
    result = execute(execution_plan)
    return result.run_root


__all__ = ["run"]
