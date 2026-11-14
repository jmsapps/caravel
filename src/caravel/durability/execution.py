"""Bounded durable invocation of a partition-scheduled pipeline.

One logical job spans many invocations. Each invocation resumes the same
durable state, executes eligible partition-step pairs until its budget runs
out, and returns a structured status. Completed work is restored from committed
output rather than recomputed.

Yielding is cooperative: the runner cannot interrupt arbitrary user code, so it
declines to *start* a unit that cannot finish within the remaining budget. Hard
termination stays correct through replay.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, Sequence

from ..plugins.api import (
    NodeFacts,
    PluginFailureError,
    PluginSet,
    RunEvent,
    RunFacts,
    RunOutcome,
    validate_plugins,
)
from ..inputs import InputPartition, StepInputs
from ..retry import (
    DEFAULT_INVOCATION_RETRY_ALLOWANCE,
    RetryBudgetExhaustedError,
    RetryCancelledError,
    RetryOptions,
    _active_retry_control,
    retry_operation,
)
from ..types import SOURCE_FIELD, Dataset, StepContext
from .commits import BatchPublisher
from .identity import logical_output_id
from .output import AttemptOutputStore, OutputReference
from .leases import LeaseHolder, LeasePolicy, acquire, validate_concurrency
from .scheduler import (
    DurableNode,
    DurablePlan,
    UnsupportedDurableDeclarationError,
    declaration_fingerprint,
    next_eligible,
    next_reclaimable,
)
from .sources import (
    AGGREGATE_PARTITION_ID,
    IncrementalInput,
    PartitionSource,
    SnapshotInput,
    SourcePartition,
)
from .sqlite import SQLiteDurableStore
from .types import (
    DependencyEdge,
    DiscoveredPartition,
    DiscoveredWorkUnit,
    DurabilityError,
    FenceViolationError,
    GenerationConflictError,
    InvalidTransitionError,
    WorkUnitUnavailableError,
    SourceMode,
    WorkUnitKey,
    WorkUnitState,
    validate_identifier,
)

RunStatus = Literal["completed", "yielded", "caught_up", "failed"]

_BATCH_ID_WIDTH = 6


class DurableExecutionError(DurabilityError):
    """Raised when a durable invocation cannot proceed safely."""


class RequiredWorkFailedError(DurabilityError):
    """Raised when required work failed and its dependents can never open."""


@dataclass(frozen=True)
class ExecutionBudget:
    """An optional monotonic time budget for one invocation."""

    seconds: float | None = None
    shutdown_margin_seconds: float = 0.0
    max_unit_seconds: float = 0.0

    def __post_init__(self) -> None:
        for name in ("seconds", "shutdown_margin_seconds", "max_unit_seconds"):
            value = getattr(self, name)
            if value is None:
                continue
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"ExecutionBudget.{name} must be a number or None.")
            if value < 0:
                raise ValueError(f"ExecutionBudget.{name} must not be negative.")
        if self.seconds is not None:
            required = float(self.shutdown_margin_seconds) + float(self.max_unit_seconds)
            if required > float(self.seconds):
                raise ValueError(
                    "ExecutionBudget rejects a configuration where no unit can fit: "
                    f"max_unit_seconds + shutdown_margin_seconds ({required}) exceeds "
                    f"seconds ({self.seconds})."
                )


@dataclass(frozen=True)
class ExecutionPolicy:
    """Concurrency policy for one durable job.

    One worker is the default. Concurrent workers require an explicit
    :class:`~caravel.durability.leases.LeasePolicy`, because without fenced
    leases there is nothing to stop a stalled worker from committing after
    another has taken its work over.
    """

    workers: int = 1
    leases: LeasePolicy | None = None
    max_retry_attempts: int = DEFAULT_INVOCATION_RETRY_ALLOWANCE
    """The invocation-wide retry allowance shared by every explicitly enabled
    retry boundary. Initial attempts never consume it; only retries do. It is
    finite by default so nested policies cannot multiply attempts without
    bound, and exhausting it is terminal for this invocation's retry chain."""

    def __post_init__(self) -> None:
        if not isinstance(self.workers, int) or isinstance(self.workers, bool):
            raise TypeError("ExecutionPolicy.workers must be an integer.")
        if self.workers < 1:
            raise ValueError("ExecutionPolicy.workers must be at least 1.")
        if not isinstance(self.max_retry_attempts, int) or isinstance(
            self.max_retry_attempts, bool
        ):
            raise TypeError("ExecutionPolicy.max_retry_attempts must be an integer.")
        if self.max_retry_attempts < 0:
            raise ValueError("ExecutionPolicy.max_retry_attempts must not be negative.")
        if self.leases is not None and not isinstance(self.leases, LeasePolicy):
            raise TypeError("ExecutionPolicy.leases must be a LeasePolicy or None.")
        if self.workers > 1 and self.leases is None:
            raise UnsupportedDurableDeclarationError(
                "Concurrent workers require an explicit LeasePolicy. Without fenced "
                "leases a stalled worker could commit after its work was taken over."
            )

    @property
    def lease_policy(self) -> LeasePolicy:
        """The effective lease policy, defaulting for single-worker runs."""
        return self.leases if self.leases is not None else LeasePolicy()

    @property
    def exclusive(self) -> bool:
        """Whether this policy guarantees exactly one live worker."""
        return self.workers == 1


@dataclass(frozen=True)
class DurableRunResult:
    """The structured outcome of one bounded invocation."""

    status: RunStatus
    pipeline_id: str
    job_id: str
    invocation_id: str
    committed_batches: int = 0
    active_node_id: str | None = None
    error_type: str | None = None
    restart_safe: bool = True
    """Whether this run's storage can actually honour a resume. False for
    ephemeral storage, so a result never implies durability it lacks."""


@dataclass(frozen=True)
class SourcePartitionView:
    """One discovered source partition, readable on demand.

    An all-input step declared before any other step has no committed upstream
    output; its input scope is the source itself. It still receives references
    and loads them lazily, so whole-run work stays bounded.
    """

    partition_id: str
    count: int
    record: SourcePartition
    source: PartitionSource

    def load(self) -> Any:
        return self.source.load(self.record)


@dataclass(frozen=True)
class CommittedPartitionView:
    """One committed upstream partition, readable without materializing it."""

    partition_id: str
    count: int
    reference: OutputReference
    output_store: AttemptOutputStore
    dataset: Dataset

    def load(self) -> Any:
        """Load this partition's committed payload on demand."""
        return self.output_store.load(self.reference, dataset=self.dataset)


def _batch_id(index: int) -> str:
    return f"batch-{index:0{_BATCH_ID_WIDTH}d}"


@dataclass(frozen=True)
class _HandleSpec:
    """One input handle before it is bound to a durable batch identity."""

    partition_id: str
    count: int
    loader: Callable[[], Any]


def _payload_spec(partition_id: str, count: int, payload: Any) -> _HandleSpec:
    """Describe an already-resolved payload as one handle spec."""
    return _HandleSpec(partition_id=partition_id, count=count, loader=lambda: payload)


def _step_inputs(batch_id: str, specs: Sequence[_HandleSpec]) -> StepInputs:
    """Bind handle specs to one durable batch and wrap them as processor inputs."""
    return StepInputs(
        batch_id=batch_id,
        handles=tuple(
            InputPartition(
                partition_id=spec.partition_id,
                batch_id=batch_id,
                count=spec.count,
                loader=spec.loader,
            )
            for spec in specs
        ),
    )


class _Deadline:
    """A monotonic invocation deadline with a shutdown margin."""

    def __init__(self, budget: ExecutionBudget) -> None:
        self._budget = budget
        self._started = time.monotonic()

    @property
    def remaining(self) -> float:
        if self._budget.seconds is None:
            return float("inf")
        return self._budget.seconds - (time.monotonic() - self._started)

    def can_start_unit(self) -> bool:
        """Whether another unit may start without overrunning the budget."""
        if self._budget.seconds is None:
            return True
        needed = float(self._budget.max_unit_seconds) + float(self._budget.shutdown_margin_seconds)
        return self.remaining > needed


class _InvocationControls:
    """The invocation-wide execution controls every managed retry inherits.

    One instance exists per invocation. It owns the monotonic deadline, the
    cancellation flag, and the shared retry allowance, so explicitly nested
    retry policies cannot multiply attempts past the invocation's limits.
    Mutable allowance accounting is synchronized, because a worker may run
    callbacks on more than one thread. A separate process reconstructs its own
    controls rather than sharing these.
    """

    def __init__(self, *, deadline: _Deadline, allowance: int, shutdown_margin: float) -> None:
        self._deadline = deadline
        self._shutdown_margin = float(shutdown_margin)
        self._lock = threading.Lock()
        self._remaining = allowance
        self._cancelled = False
        self.cancel_event = threading.Event()

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            self.cancel_event.set()

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    @property
    def remaining_retries(self) -> int:
        with self._lock:
            return self._remaining

    def charge(self, scope: str) -> None:
        """Consume one shared retry attempt, or refuse the whole retry chain."""
        with self._lock:
            if self._remaining <= 0:
                raise RetryBudgetExhaustedError(
                    f"Scope '{scope}' cannot retry: this invocation's shared retry "
                    "allowance is exhausted. Raise ExecutionPolicy.max_retry_attempts "
                    "or reduce nested retry boundaries."
                )
            self._remaining -= 1

    def check_live(self, scope: str) -> None:
        if self.cancelled:
            raise RetryCancelledError(f"Scope '{scope}' was cancelled.")
        if self._deadline.remaining <= self._shutdown_margin:
            raise RetryCancelledError(
                f"Scope '{scope}' has no remaining invocation time; it stopped at a "
                "durable boundary instead of extending past its deadline."
            )

    def fits(self, delay: float) -> bool:
        return self._deadline.remaining > delay + self._shutdown_margin

    def bind(
        self,
        *,
        scope: str,
        owner_id: str,
        fence_token: int | None,
        check_claim: Callable[[], None] | None = None,
    ) -> _BoundRetryControl:
        return _BoundRetryControl(
            self, scope=scope, owner_id=owner_id, fence_token=fence_token, check_claim=check_claim
        )


class _BoundRetryControl:
    """Invocation controls bound to one claimed callback's ownership facts.

    The owner id and fence token are the ones acquired at claim time. They are
    recorded, never re-read: a worker that has already been fenced out must not
    be able to adopt the winner's authority by consulting the store again.
    """

    def __init__(
        self,
        controls: _InvocationControls,
        *,
        scope: str,
        owner_id: str,
        fence_token: int | None,
        check_claim: Callable[[], None] | None = None,
    ) -> None:
        self._controls = controls
        self._scope = scope
        self.owner_id = owner_id
        self.fence_token = fence_token
        self._check_claim = check_claim

    def _check_live(self) -> None:
        self._controls.check_live(self._scope)
        if self._check_claim is not None:
            self._check_claim()

    def before_attempt(self, attempt: int) -> None:
        """Charge retries only; an initial attempt never consumes the allowance."""
        self._check_live()
        if attempt > 1:
            self._controls.charge(self._scope)

    def wait(self, seconds: float) -> None:
        self._check_live()
        if not self._controls.fits(seconds):
            raise RetryCancelledError(
                f"Scope '{self._scope}' has insufficient remaining time for another "
                "attempt; it stopped at a durable boundary instead."
            )
        until = time.monotonic() + seconds
        while (remaining := until - time.monotonic()) > 0:
            self._controls.cancel_event.wait(min(remaining, 0.1))
            self._check_live()
        self._check_live()


class DurableInvocation:
    """Executes eligible partition-step work for one bounded invocation."""

    def __init__(
        self,
        *,
        plan: DurablePlan,
        store: SQLiteDurableStore,
        output: AttemptOutputStore,
        source: PartitionSource,
        pipeline_id: str,
        job_id: str,
        budget: ExecutionBudget | None = None,
        policy: ExecutionPolicy | None = None,
        worker_id: str | None = None,
        params: Mapping[str, str] | None = None,
        context_factory: Callable[[StepContext], Any] | None = None,
        keep_source_tag: bool = False,
        lineage: str = "default",
        restart_safe: bool = True,
        plugins: Sequence[Any] = (),
        storage_root: str = "",
        publisher: Any = None,
        source_mode: SourceMode = "snapshot",
        snapshot_id: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.plan = plan
        self.store = store
        self.output = output
        self.source = source
        self.pipeline_id = validate_identifier("pipeline_id", pipeline_id)
        self.job_id = validate_identifier("job_id", job_id)
        self.budget = budget if budget is not None else ExecutionBudget()
        self.policy = policy if policy is not None else ExecutionPolicy()
        validate_concurrency(store, workers=self.policy.workers)
        self.worker_id = worker_id if worker_id is not None else f"worker-{uuid.uuid4().hex[:12]}"
        self.params = dict(params or {})
        self.context_factory = context_factory
        self.keep_source_tag = keep_source_tag
        self.lineage = validate_identifier("lineage", lineage)
        self.restart_safe = restart_safe
        self.storage_root = storage_root
        self.publisher_projection = publisher
        self.plugin_set: PluginSet = validate_plugins(tuple(plugins))
        if isinstance(source, SnapshotInput):
            self.source_mode: SourceMode = "snapshot"
            self.snapshot_id: str | None = source.snapshot
        elif isinstance(source, IncrementalInput):
            self.source_mode = "incremental"
            self.snapshot_id = None
        else:
            self.source_mode = source_mode
            self.snapshot_id = snapshot_id
        self.invocation_id = uuid.uuid4().hex
        self.logger = logger if logger is not None else logging.getLogger("caravel.durable")
        self.publisher = BatchPublisher(store, output)
        self.deadline = _Deadline(self.budget)
        self.controls = _InvocationControls(
            deadline=self.deadline,
            allowance=self.policy.max_retry_attempts,
            shutdown_margin=self.budget.shutdown_margin_seconds,
        )

    # -- observers and guards ----------------------------------------------

    def _run_facts(self) -> RunFacts:
        return RunFacts(
            pipeline_name=self.plan.pipeline_name,
            pipeline_id=self.pipeline_id,
            job_id=self.job_id,
            invocation_id=self.invocation_id,
            storage_root=self.storage_root,
            restart_safe=self.restart_safe,
        )

    def _node_facts(self, node: DurableNode, partition_id: str) -> NodeFacts:
        return NodeFacts(
            node_id=node.node_id,
            partition_id=partition_id,
            stage_index=node.stage_index,
            stage_name=node.stage_name,
            entry_index=node.entry_index,
            name=node.step.name or node.node_id,
            input_scope=node.input_scope,
            dataset_type=type(node.step.dataset).__name__,
            dataset_name=getattr(node.step.dataset, "name", ""),
        )

    def _emit(
        self,
        kind: Any,
        *,
        node: NodeFacts | None = None,
        error_type: str | None = None,
    ) -> None:
        """Deliver one lifecycle event after a committed transition.

        A required observer's failure fails the invocation; a best-effort
        observer's failure never does. Observers cannot change what already
        committed.
        """
        if not self.plugin_set.observers:
            return
        event = RunEvent(kind=kind, run=self._run_facts(), node=node, error_type=error_type)
        for plugin_id, observer, criticality in self.plugin_set.observers:
            try:
                observer.on_event(event)
            except Exception as error:
                if criticality == "required":
                    raise PluginFailureError(
                        f"Required observer '{plugin_id}' failed on '{kind}': "
                        f"{type(error).__name__}."
                    ) from error
                self.logger.warning(
                    "Observer %s failed on %s: %s", plugin_id, kind, type(error).__name__
                )

    # -- input resolution --------------------------------------------------

    def _upstream_partitions(self, node: DurableNode) -> tuple[str, ...]:
        """Return the partition ids the previous node produced."""
        previous = self.plan.previous(node)
        if previous is None:
            return ()
        return self.store.work_unit_partitions(self.job_id, previous.node_id)

    def _resolve_input(self, node: DurableNode, partition_id: str) -> Any:
        previous = self.plan.previous(node)

        if node.input_scope == "all":
            # With an upstream, a barrier reads committed references. Declared
            # first, its "all input" is the source itself: returning an empty
            # reader there would silently aggregate nothing.
            if previous is None:
                return self._source_views()
            return [
                view
                for upstream in self._upstream_partitions(node)
                for view in self._views_for(previous, upstream)
            ]

        if previous is None:
            partition = self.store.load_partition(self.job_id, partition_id)
            if partition is None:
                raise InvalidTransitionError(
                    f"Partition '{partition_id}' is not registered for job '{self.job_id}'."
                )
            return self.source.load(
                SourcePartition(
                    partition_id=partition.partition_id,
                    input_ref=partition.input_ref,
                    input_version=partition.input_version,
                )
            )

        if node.kind == "merge":
            return self._load_committed(self._route_final_step(node, partition_id), partition_id)

        if previous is None:
            raise InvalidTransitionError(f"Node '{node.node_id}' has no upstream to read.")
        return self._load_committed(previous, partition_id)

    def _source_views(self) -> list[Any]:
        """Lazy views over every discovered source partition."""
        views: list[Any] = []
        for partition_id in self.store.list_partitions(self.job_id):
            if partition_id == AGGREGATE_PARTITION_ID:
                continue
            record = self.store.load_partition(self.job_id, partition_id)
            if record is None:
                continue
            views.append(
                SourcePartitionView(
                    partition_id=partition_id,
                    count=1,
                    record=SourcePartition(
                        partition_id=record.partition_id,
                        input_ref=record.input_ref,
                        input_version=record.input_version,
                    ),
                    source=self.source,
                )
            )
        return views

    def _views_for(self, previous: DurableNode, partition_id: str) -> list[CommittedPartitionView]:
        key = WorkUnitKey(self.job_id, previous.node_id, partition_id)
        return [
            CommittedPartitionView(
                partition_id=partition_id,
                count=reference.count,
                reference=reference,
                output_store=self.output,
                dataset=previous.step.dataset,
            )
            for reference in self.publisher.committed_references(key)
        ]

    def _committed_route(self, branch: DurableNode, partition_id: str) -> str:
        """Read the authoritative route for a partition; never re-decide it."""
        unit = self.store.load_work_unit(WorkUnitKey(self.job_id, branch.node_id, partition_id))
        if unit is None or unit.route_key is None:
            raise InvalidTransitionError(
                f"Partition '{partition_id}' has no committed route for branch '{branch.node_id}'."
            )
        return unit.route_key

    def _route_final_step(self, merge: DurableNode, partition_id: str) -> DurableNode:
        assert merge.branch_node_id is not None
        branch = self.plan.node_by_id(merge.branch_node_id)
        return branch.route_chain(self._committed_route(branch, partition_id))[-1]

    def _load_committed(self, previous: DurableNode, partition_id: str) -> Any:
        """Restore an upstream partition's committed output."""
        key = WorkUnitKey(self.job_id, previous.node_id, partition_id)
        payloads = self.publisher.load_work_unit(key, dataset=previous.step.dataset)
        if len(payloads) == 1:
            return payloads[0]
        if payloads and all(isinstance(item, list) for item in payloads):
            combined: list[Any] = []
            for item in payloads:
                combined.extend(item)
            return combined
        return payloads

    # -- execution ---------------------------------------------------------

    def _step_context(self, node: DurableNode, partition_id: str) -> StepContext:
        return StepContext(
            run_root=str(self.output.root),
            pipeline_name=self.plan.pipeline_name,
            run_id=self.job_id,
            stage_index=node.stage_index,
            stage_name=node.stage_name,
            step_index=node.entry_index,
            step_name=node.step.name or node.node_id,
            persist=node.step.persist,
            step_dir=str(self.output.root),
            prev_step_dir=None,
            logger=self.logger,
            params=dict(self.params),
        )

    @staticmethod
    def _without_source_tag(payload: Any) -> Any:
        """Remove the routing lineage tag from a payload before it is committed.

        Preparing a payload before committing it is not in tension with
        immutable output: what gets committed is immutable, and preparation
        happens first. Source identity is not lost — it is recorded in the
        manifest's partition metadata, which is where lineage belongs.
        """
        if isinstance(payload, dict):
            if SOURCE_FIELD in payload:
                return {key: value for key, value in payload.items() if key != SOURCE_FIELD}
            return {
                key: DurableInvocation._without_source_tag(value)
                if isinstance(value, dict)
                else value
                for key, value in payload.items()
            }
        return payload

    def _prepare_output(self, produced: Any) -> Any:
        """Prepare a processor's return value for durable publication."""
        if self.keep_source_tag:
            return produced
        return self._without_source_tag(produced)

    def _publish(self, node: DurableNode, key: WorkUnitKey) -> None:
        """Materialize a sealed unit, so a partition is readable before the job ends.

        Publication is a projection of committed state, so a failure here is
        logged rather than failing a committed unit. The unit stays
        sealed-but-unpublished and the next invocation republishes it.
        """
        if self.publisher_projection is None:
            return
        try:
            self.publisher_projection.publish_unit(node, key)
        except Exception as error:
            self.logger.warning(
                "Publishing %s/%s failed (%s); it will be republished.",
                key.node_id,
                key.partition_id,
                type(error).__name__,
            )

    def _materialize_context(self, base: StepContext) -> Any:
        """Hand the step its context, wrapped by an application factory if given.

        The factory sees the framework context and returns whatever the
        application wants steps to receive. It cannot change scheduling,
        progress, or commits.
        """
        if self.context_factory is None:
            return base
        return self.context_factory(base)

    def _input_identity(self, node: DurableNode, partition_id: str) -> str:
        """Return the declared identity a processor sees for this unit's input.

        A real source partition addresses itself, hierarchical ids included. An
        input derived from an upstream barrier is addressed by that step's
        declared output name, so the reserved aggregate execution id never
        reaches a key a processor produces.
        """
        if partition_id != AGGREGATE_PARTITION_ID:
            return partition_id
        previous = self.plan.previous(node)
        if previous is None:  # pragma: no cover - the aggregate partition always has an upstream
            return partition_id
        return logical_output_id(previous, partition_id)

    def _view_specs(self, node: DurableNode, views: Sequence[Any]) -> tuple[_HandleSpec, ...]:
        """Describe upstream views as handle specs with their declared identities."""
        previous = self.plan.previous(node)
        return tuple(
            _HandleSpec(
                partition_id=(
                    view.partition_id
                    if previous is None or view.partition_id != AGGREGATE_PARTITION_ID
                    else logical_output_id(previous, view.partition_id)
                ),
                count=view.count,
                loader=view.load,
            )
            for view in views
        )

    def _batches(
        self, node: DurableNode, partition_id: str, resolved: Any
    ) -> list[tuple[str, StepInputs, int]]:
        """Split a unit's input into durable batches of processor inputs.

        ``batch_size=None`` executes the selected scope as one durable unit. In
        partition scope a positive size requires input that can be read in
        bounded, replayable pieces; anything else is rejected before the
        processor runs. In all scope a positive size slices the upstream
        partition *views*, never their loaded payloads, so aggregating a large
        job stays bounded in memory.

        Every batch, in every scope, is one ``StepInputs`` holding
        ``InputPartition`` handles. Payloads load through a handle, so building
        the batches materializes nothing.
        """
        size = node.step.batch_size

        if node.input_scope == "all":
            specs = self._view_specs(node, resolved)
            if size is None:
                return [(_batch_id(0), _step_inputs(_batch_id(0), specs), len(specs))]
            slices = [specs[start : start + size] for start in range(0, len(specs), size)] or [()]
            return [
                (_batch_id(index), _step_inputs(_batch_id(index), part), len(part))
                for index, part in enumerate(slices)
            ]

        identity = self._input_identity(node, partition_id)
        payload = resolved
        if size is None:
            count = len(payload) if isinstance(payload, list) else 1
            return [
                (
                    _batch_id(0),
                    _step_inputs(_batch_id(0), (_payload_spec(identity, count, payload),)),
                    count,
                )
            ]
        if isinstance(payload, (str, bytes)) or not isinstance(payload, Sequence):
            raise UnsupportedDurableDeclarationError(
                f"Step '{node.step.name}' declares batch_size={size} but its input is "
                f"{type(payload).__name__}, which has no bounded replayable read. Use "
                "batch_size=None or supply a sequence source."
            )
        records = list(payload)
        batched = [records[start : start + size] for start in range(0, len(records), size)] or [[]]
        return [
            (
                _batch_id(index),
                _step_inputs(_batch_id(index), (_payload_spec(identity, len(part), part),)),
                len(part),
            )
            for index, part in enumerate(batched)
        ]

    @staticmethod
    def _with_retry(operation: Callable[[], Any], options: RetryOptions | None) -> Any:
        """Run ``operation`` under ``options``, or once when retry is disabled.

        Every supported boundary funnels through the one shared retry loop; no
        boundary reimplements attempts, backoff, or classification.
        """
        if options is None:
            return operation()
        result, _ = retry_operation(operation=operation, retry=options)
        return result

    def _execute_unit(self, unit: WorkUnitState, *, reclaim: bool = False) -> int:
        """Claim, execute, publish, and seal one partition-step pair.

        ``reclaim`` takes over a unit left unfinished by a prior invocation,
        whether abandoned mid-execution or failed. It is permitted only under a
        single-worker policy, which guarantees this invocation is the only live
        worker. The displaced owner's fence token becomes stale, so its writes
        can never land.
        """
        node = self.plan.node_by_id(unit.key.node_id)
        lease: LeaseHolder = acquire(
            self.store,
            unit.key,
            owner_id=self.worker_id,
            expected_generation=unit.generation,
            policy=self.policy.lease_policy,
            # Seizing a live lease is valid only when this is the sole worker.
            # With concurrent workers, only an expired lease may be taken over.
            exclusive=reclaim and self.policy.exclusive,
        )
        claimed = lease.state

        facts = self._node_facts(node, unit.key.partition_id)
        self._emit("node_started", node=facts)
        resolved = self._resolve_input(node, unit.key.partition_id)
        context = self._materialize_context(self._step_context(node, unit.key.partition_id))

        if node.kind == "branch":
            committed = self._execute_branch(node, unit, lease, claimed, resolved, context)
            self._publish(node, unit.key)
            self._emit("node_completed", node=facts)
            return committed

        scope = f"{node.node_id}/{unit.key.partition_id}"
        # Bind this callback's acquired ownership facts for the duration of the
        # claim, so a retry_operation() written inside the callback inherits the
        # deadline, cancellation, and shared allowance with no context argument.
        token = _active_retry_control.set(
            self.controls.bind(
                scope=scope,
                owner_id=self.worker_id,
                fence_token=lease.fence_token,
                check_claim=lambda: self.store.check_claim(
                    unit.key, owner_id=self.worker_id, fence_token=claimed.fence_token
                ),
            )
        )
        try:
            committed, state = self._run_batches(node, unit, lease, claimed, resolved, context)
        finally:
            _active_retry_control.reset(token)

        lease.observe(state)
        if not state.input_exhausted:
            state = self.store.mark_input_exhausted(
                unit.key,
                owner_id=self.worker_id,
                fence_token=state.fence_token,
                expected_generation=state.generation,
            )
        self.store.seal_work_unit(
            unit.key,
            owner_id=self.worker_id,
            fence_token=state.fence_token,
            expected_generation=state.generation,
        )
        self._publish(node, unit.key)
        self._emit("node_completed", node=facts)
        return committed

    def _run_batches(
        self,
        node: DurableNode,
        unit: WorkUnitState,
        lease: LeaseHolder,
        claimed: WorkUnitState,
        resolved: Any,
        context: Any,
    ) -> tuple[int, WorkUnitState]:
        """Run this unit's batches under its stage and step retries.

        The two boundaries nest stage -> step, both strictly inside the lease
        this worker already holds, so the inner retry exhausts first and no
        retry re-acquires authority or recompiles the declaration. A smaller
        scope is retried by calling ``retry_operation()`` inside the processor;
        the engine adds no boundary of its own around one loop iteration.
        """
        batches = self._batches(node, unit.key.partition_id, resolved)
        processor = node.step.fn
        progress: dict[str, Any] = {"committed": 0, "state": claimed}

        def run_batch(batch_id: str, inputs: StepInputs, record_count: int) -> None:
            def attempt() -> Any:
                # Authority is fixed at claim time. This worker presents the
                # fence token it acquired and the generation it last observed
                # from its own commits; it never re-reads either from the
                # store. Refreshing them would let a worker that has already
                # been fenced out adopt the winner's authority and commit.
                #
                # Idempotency does not need a re-read: publish() resolves an
                # already-committed batch id from the manifest, so an attempt
                # that published before failing is observed, not repeated.
                produced = self._prepare_output(processor(inputs, context=context))
                observed: WorkUnitState = progress["state"]
                return self.publisher.publish(
                    unit.key,
                    owner_id=self.worker_id,
                    fence_token=lease.fence_token,
                    expected_generation=observed.generation,
                    batch_id=batch_id,
                    dataset=node.step.dataset,
                    payload=produced,
                    next_cursor=batch_id,
                    record_count=record_count,
                )

            commit = attempt()
            progress["state"] = commit.state
            progress["committed"] = int(progress["committed"]) + 1

        def run_all() -> None:
            # Re-read on every entry: an outer retry must skip what an earlier
            # pass already committed rather than republish it.
            already = {record.batch_id for record in self.store.work_unit_batches(unit.key)}
            for batch_id, inputs, record_count in batches:
                if batch_id in already:
                    continue
                run_batch(batch_id, inputs, record_count)

        self._with_retry(
            lambda: self._with_retry(run_all, node.step.retry),
            node.stage_retry,
        )
        return int(progress["committed"]), progress["state"]

    def _execute_branch(
        self,
        node: DurableNode,
        unit: WorkUnitState,
        lease: LeaseHolder,
        claimed: WorkUnitState,
        payload: Any,
        context: Any,
    ) -> int:
        """Decide this partition's route and commit the decision with its work.

        The predicate runs here, at the branch boundary, because it may read
        values earlier steps produced. Its result and the route's work units
        land in one transaction, so a crash either leaves the partition
        unrouted or fully routed.
        """
        partition_id = unit.key.partition_id
        assert node.branch is not None
        existing = self.store.load_work_unit(unit.key)
        route_key = (
            existing.route_key
            if existing is not None and existing.route_key is not None
            else node.branch.route_key_for(partition_id, payload)
        )
        chain = node.route_chain(route_key)

        # The branch commits its partition's payload unchanged, so the route's
        # first step reads it like any other upstream output.
        commit = self.publisher.publish(
            unit.key,
            owner_id=self.worker_id,
            fence_token=lease.fence_token,
            expected_generation=claimed.generation,
            batch_id=_batch_id(0),
            dataset=node.step.dataset,
            payload=self._prepare_output(payload),
            next_cursor=_batch_id(0),
            record_count=1,
        )
        state = commit.state
        if not state.input_exhausted:
            state = self.store.mark_input_exhausted(
                unit.key,
                owner_id=self.worker_id,
                fence_token=state.fence_token,
                expected_generation=state.generation,
            )

        units = tuple(
            DiscoveredWorkUnit(
                node_id=member.node_id,
                step_revision=member.step.revision,
                params_fingerprint=declaration_fingerprint(self.plan),
                input_scope="partition",
            )
            for member in chain
        )
        edges = [
            DependencyEdge(
                node_id=member.node_id,
                partition_id=partition_id,
                depends_on_node_id=(chain[position - 1].node_id if position else node.node_id),
                depends_on_partition_id=partition_id,
            )
            for position, member in enumerate(chain)
        ]
        assert node.merge_node_id is not None
        edges.append(
            DependencyEdge(
                node_id=node.merge_node_id,
                partition_id=partition_id,
                depends_on_node_id=chain[-1].node_id,
                depends_on_partition_id=partition_id,
            )
        )

        self.store.seal_branch(
            unit.key,
            owner_id=self.worker_id,
            fence_token=state.fence_token,
            expected_generation=state.generation,
            route_key=route_key,
            partitions=(
                DiscoveredPartition(
                    partition_id=partition_id,
                    input_ref=self._partition_input_ref(partition_id),
                    work_units=units,
                ),
            ),
            dependencies=edges,
        )
        return 1

    def _partition_input_ref(self, partition_id: str) -> str:
        record = self.store.load_partition(self.job_id, partition_id)
        if record is None:
            raise InvalidTransitionError(
                f"Partition '{partition_id}' is not registered for job '{self.job_id}'."
            )
        return record.input_ref

    # -- invocation --------------------------------------------------------

    def _resolve_range(self) -> tuple[str | None, str | None] | None:
        """Return this job's bounds, allocating them only if it has none.

        Bounds are read from durable state first. A job whose bounds are
        already persisted never re-asks the adapter, which is what stops a
        retried job from skipping or expanding its range. Returns None when the
        live source is caught up and no range exists yet.
        """
        if not isinstance(self.source, IncrementalInput):
            return (None, None)

        existing = self.store.load_job(self.job_id)
        if existing is not None and existing.range_start is not None:
            if existing.range_end is None:
                raise DurableExecutionError(f"Durable job '{self.job_id}' has an incomplete range.")
            self.source.adopt(existing.range_start, existing.range_end)
            return (existing.range_start, existing.range_end)

        checkpoint = self.store.load_checkpoint(self.pipeline_id, self.lineage)
        after = checkpoint.completed_position if checkpoint is not None else None
        allocated = self.source.allocate(after)
        if allocated is None:
            return None
        return (allocated.start, allocated.end)

    def _reject_foreign_declaration(self) -> None:
        """Fail a persisted job whose declaration contract is not this one.

        This runs before the source is consulted and before any durable
        mutation, so a job written under an older declaration fingerprint can
        never allocate a range, re-pin a snapshot, register discovery, or run a
        processor. Stored state is not reinterpreted under a new contract; the
        remedy is a fresh job id. ``initialize_job`` keeps the same check as the
        transactional backstop for a concurrent writer.
        """
        existing = self.store.load_job(self.job_id)
        if existing is None:
            return
        current = declaration_fingerprint(self.plan)
        if existing.declaration_fingerprint != current:
            raise InvalidTransitionError(
                f"Durable job '{self.job_id}' already exists with a different "
                "declaration fingerprint. Its persisted scope, revisions, or output "
                "identities describe a declaration this run does not match, and stored "
                "state is never reinterpreted under a new contract; start a new job id "
                "instead."
            )

    def _ensure_job(self) -> Any:
        self._reject_foreign_declaration()
        existing = self.store.load_job(self.job_id)
        if existing is not None and existing.status == "completed":
            # A finished job is answered from its own durable state. Consulting
            # the source here would let an aged-out range make a completed job
            # permanently un-queryable, and re-pinning a moved snapshot is a new
            # job's concern. Declaration identity is still validated, because a
            # mismatch must fail rather than silently report someone else's
            # completed work as this declaration's.
            return self.store.initialize_job(
                self.job_id,
                pipeline_id=self.pipeline_id,
                source_mode=self.source_mode,
                declaration_fingerprint=declaration_fingerprint(self.plan),
                lineage=self.lineage,
                snapshot_id=self.snapshot_id,
                range_start=existing.range_start,
                range_end=existing.range_end,
            )

        bounds = self._resolve_range()
        if bounds is None:
            return None
        range_start, range_end = bounds

        if isinstance(self.source, SnapshotInput):
            # Reject a moved pin before any processing or durable mutation.
            self.source.validate_pin()

        job = self.store.initialize_job(
            self.job_id,
            pipeline_id=self.pipeline_id,
            source_mode=self.source_mode,
            declaration_fingerprint=declaration_fingerprint(self.plan),
            lineage=self.lineage,
            snapshot_id=self.snapshot_id,
            range_start=range_start,
            range_end=range_end,
        )
        if not job.discovery_sealed:
            from .scheduler import register_plan_discovery

            try:
                job = register_plan_discovery(self.store, job, self.plan, self.source.discover())
            except (GenerationConflictError, InvalidTransitionError):
                # A competing worker registered discovery first. Exactly one
                # registration commits; the loser adopts its result.
                reloaded = self.store.load_job(self.job_id)
                if reloaded is None or not reloaded.discovery_sealed:
                    raise
                job = reloaded
        return job

    def run(self) -> DurableRunResult:
        """Execute eligible work until the job completes or the budget runs out.

        Run guards enter in plugin order before any user code and exit in
        reverse order, with teardown attempted for every guard whose startup
        completed.
        """
        entered: list[tuple[str, Any]] = []
        # The invocation's execution controls are installed here, at entry, and
        # reset unconditionally on the way out, so a failure or cancellation
        # never leaks a deadline or a spent allowance into the next invocation.
        self.deadline = _Deadline(self.budget)
        self.controls = _InvocationControls(
            deadline=self.deadline,
            allowance=self.policy.max_retry_attempts,
            shutdown_margin=self.budget.shutdown_margin_seconds,
        )
        token = _active_retry_control.set(
            self.controls.bind(
                scope=f"{self.job_id}/{self.invocation_id}",
                owner_id=self.worker_id,
                fence_token=None,
            )
        )
        try:
            for plugin_id, guard in self.plugin_set.guards:
                guard.enter(self._run_facts())
                entered.append((plugin_id, guard))
            self._emit("run_started")
            result = self._run_units()
        except BaseException as error:
            self._emit("run_failed", error_type=type(error).__name__)
            self._exit_guards(entered, RunOutcome(status="failed", error_type=type(error).__name__))
            raise
        finally:
            _active_retry_control.reset(token)
        self._emit("run_completed")
        self._exit_guards(entered, RunOutcome(status=result.status))
        return result

    def _exit_guards(self, entered: list[tuple[str, Any]], outcome: RunOutcome) -> None:
        """Exit guards in reverse order, attempting every one before reporting."""
        failures: list[str] = []
        facts = self._run_facts()
        for plugin_id, guard in reversed(entered):
            try:
                guard.exit(facts, outcome)
            except Exception as error:
                failures.append(f"{plugin_id}: {type(error).__name__}")
        if failures:
            raise PluginFailureError(f"Run guard teardown failed: {', '.join(failures)}.")

    def _run_units(self) -> DurableRunResult:
        job = self._ensure_job()
        if job is None:
            # The live source has no new range. The pipeline stays open.
            return DurableRunResult(
                status="caught_up",
                pipeline_id=self.pipeline_id,
                job_id=self.job_id,
                invocation_id=self.invocation_id,
                restart_safe=self.restart_safe,
            )
        if job.status == "completed":
            return DurableRunResult(
                status="completed",
                pipeline_id=self.pipeline_id,
                job_id=self.job_id,
                invocation_id=self.invocation_id,
                restart_safe=self.restart_safe,
            )

        deadline = self.deadline
        committed = 0
        active_node_id: str | None = None

        while True:
            reclaim = False
            unit = next_eligible(self.store, self.job_id)
            if unit is None:
                unit = next_reclaimable(self.store, self.job_id, include_live=self.policy.exclusive)
                reclaim = unit is not None
            if unit is None:
                break
            if not deadline.can_start_unit():
                return DurableRunResult(
                    status="yielded",
                    pipeline_id=self.pipeline_id,
                    job_id=self.job_id,
                    invocation_id=self.invocation_id,
                    committed_batches=committed,
                    active_node_id=unit.key.node_id,
                    restart_safe=self.restart_safe,
                )
            active_node_id = unit.key.node_id
            try:
                committed += self._execute_unit(unit, reclaim=reclaim)
            except (
                FenceViolationError,
                GenerationConflictError,
                WorkUnitUnavailableError,
            ):
                # Another worker won this unit. Reload and take the next one
                # rather than contending; at most one worker can hold it.
                if self.policy.exclusive:
                    raise
                continue
            except DurabilityError:
                raise
            except Exception as error:
                current = self.store.load_work_unit(unit.key)
                if current is not None and current.status == "executing":
                    self.store.fail_work_unit(
                        unit.key,
                        owner_id=self.worker_id,
                        fence_token=current.fence_token,
                        expected_generation=current.generation,
                        error_type=type(error).__name__,
                    )
                raise

        return self._finalize(committed, active_node_id)

    def _finalize(self, committed: int, active_node_id: str | None) -> DurableRunResult:
        """Complete the job, or report why it cannot complete yet."""
        job = self.store.load_job(self.job_id)
        if job is None:
            raise DurableExecutionError(f"Durable job '{self.job_id}' disappeared.")

        failed = self.store.unsealed_statuses(self.job_id)
        if "failed" in failed:
            return DurableRunResult(
                status="failed",
                pipeline_id=self.pipeline_id,
                job_id=self.job_id,
                invocation_id=self.invocation_id,
                committed_batches=committed,
                active_node_id=active_node_id,
                error_type="RequiredWorkFailedError",
                restart_safe=self.restart_safe,
            )
        if failed:
            return DurableRunResult(
                status="yielded",
                pipeline_id=self.pipeline_id,
                job_id=self.job_id,
                invocation_id=self.invocation_id,
                committed_batches=committed,
                active_node_id=active_node_id,
                restart_safe=self.restart_safe,
            )

        checkpoint_position = job.range_end if job.source_mode == "incremental" else None
        try:
            self.store.complete_job(
                self.job_id,
                expected_generation=job.generation,
                checkpoint_position=checkpoint_position,
            )
        except (InvalidTransitionError, GenerationConflictError):
            # A competing worker may have completed the job between our read
            # and our write. Exactly one completion commits; the loser observes
            # it rather than failing.
            current = self.store.load_job(self.job_id)
            if current is None or current.status != "completed":
                raise
        self._publish_completion()
        return DurableRunResult(
            status="completed",
            pipeline_id=self.pipeline_id,
            job_id=self.job_id,
            invocation_id=self.invocation_id,
            committed_batches=committed,
            active_node_id=active_node_id,
            restart_safe=self.restart_safe,
        )

    def _publish_completion(self) -> None:
        """Catch up any unpublished unit, then write the whole-job boundary last."""
        if self.publisher_projection is None:
            return
        try:
            # Drain first; write_job_manifest refuses while anything is pending,
            # so the marker can never claim a job is fully materialized early.
            self.publisher_projection.reconcile(self.job_id, self.plan.nodes)
            if self.publisher_projection.write_job_manifest(self.job_id, self.plan.nodes) is None:
                self.logger.warning(
                    "Job %s completed but publication is incomplete; "
                    "no completion marker was written.",
                    self.job_id,
                )
        except Exception as error:
            self.logger.warning(
                "Publishing job completion failed (%s); committed state is unaffected.",
                type(error).__name__,
            )


__all__ = [
    "CommittedPartitionView",
    "SourcePartitionView",
    "DurableExecutionError",
    "DurableInvocation",
    "DurableRunResult",
    "ExecutionBudget",
    "ExecutionPolicy",
    "RequiredWorkFailedError",
    "RunStatus",
]
