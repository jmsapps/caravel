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
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Literal, Mapping, Sequence

from ..plugins.api import (
    NodeFacts,
    PluginFailureError,
    PluginSet,
    RunEvent,
    RunFacts,
    RunOutcome,
    validate_plugins,
)
from ..retry import RetryBudget, RetryPolicy, retry
from ..types import SOURCE_FIELD, Dataset, StepContext
from .commits import BatchPublisher
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

    def __post_init__(self) -> None:
        if not isinstance(self.workers, int) or isinstance(self.workers, bool):
            raise TypeError("ExecutionPolicy.workers must be an integer.")
        if self.workers < 1:
            raise ValueError("ExecutionPolicy.workers must be at least 1.")
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


class CommittedDatasetReader:
    """A reader over committed partition references for an all-input step.

    An all-input step receives this reader rather than an eagerly materialized
    dataset, so whole-run work stays bounded in memory.
    """

    def __init__(self, views: Sequence[Any]) -> None:
        self._views = tuple(views)

    def partitions(self) -> Iterator[Any]:
        return iter(self._views)

    def __len__(self) -> int:
        return len(self._views)

    @property
    def is_empty(self) -> bool:
        return not self._views


def _batch_id(index: int) -> str:
    return f"batch-{index:0{_BATCH_ID_WIDTH}d}"


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
                return CommittedDatasetReader(self._source_views())
            return CommittedDatasetReader(
                [
                    view
                    for upstream in self._upstream_partitions(node)
                    for view in self._views_for(previous, upstream)
                ]
            )

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

    def _materialize_context(self, base: StepContext) -> Any:
        """Hand the step its context, wrapped by an application factory if given.

        The factory sees the framework context and returns whatever the
        application wants steps to receive. It cannot change scheduling,
        progress, or commits.
        """
        if self.context_factory is None:
            return base
        return self.context_factory(base)

    def _batches(self, node: DurableNode, payload: Any) -> list[tuple[str, Any]]:
        """Split a unit's input into durable batches.

        ``batch_size=None`` executes the selected scope as one durable unit.
        A positive size requires input that can be read in bounded, replayable
        pieces; anything else is rejected before the processor runs.
        """
        size = node.step.batch_size
        if size is None:
            return [(_batch_id(0), payload)]
        if isinstance(payload, (str, bytes)) or not isinstance(payload, Sequence):
            raise UnsupportedDurableDeclarationError(
                f"Step '{node.step.name}' declares batch_size={size} but its input is "
                f"{type(payload).__name__}, which has no bounded replayable read. Use "
                "batch_size=None or supply a sequence source."
            )
        records = list(payload)
        return [
            (_batch_id(start // size), records[start : start + size])
            for start in range(0, len(records), size)
        ] or [(_batch_id(0), [])]

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
        payload = self._resolve_input(node, unit.key.partition_id)
        context = self._materialize_context(self._step_context(node, unit.key.partition_id))

        state = claimed
        committed = 0
        batches = self._batches(node, payload)
        already = {record.batch_id for record in self.store.work_unit_batches(unit.key)}

        if node.kind == "branch":
            committed = self._execute_branch(node, unit, lease, claimed, payload, context)
            self._emit("node_completed", node=facts)
            return committed

        policy = node.step.retry if isinstance(node.step.retry, RetryPolicy) else None
        budget = RetryBudget(
            deadline_seconds=self.budget.seconds,
            shutdown_margin_seconds=self.budget.shutdown_margin_seconds,
        )

        for batch_id, batch_payload in batches:
            if batch_id in already:
                continue

            def attempt(
                batch_id: str = batch_id,
                batch_payload: Any = batch_payload,
                observed: WorkUnitState = state,
            ) -> Any:
                # Authority is fixed at claim time. This worker presents the
                # fence token it acquired and the generation it last observed
                # from its own commits; it never re-reads either from the
                # store. Refreshing them would let a worker that has already
                # been fenced out adopt the winner's authority and commit.
                #
                # Idempotency does not need a re-read: publish() resolves an
                # already-committed batch id from the manifest, so an attempt
                # that published before failing is observed, not repeated.
                produced = self._prepare_output(node.step.fn(batch_payload, context=context))
                return self.publisher.publish(
                    unit.key,
                    owner_id=self.worker_id,
                    fence_token=lease.fence_token,
                    expected_generation=observed.generation,
                    batch_id=batch_id,
                    dataset=node.step.dataset,
                    payload=produced,
                    next_cursor=batch_id,
                    record_count=(len(batch_payload) if isinstance(batch_payload, list) else 1),
                )

            if policy is None:
                commit = attempt()
            else:
                commit = retry(
                    attempt,
                    policy=policy,
                    scope=f"{node.node_id}/{unit.key.partition_id}/{batch_id}",
                    budget=budget,
                )
            state = commit.state
            committed += 1

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
        self._emit("node_completed", node=facts)
        return committed

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
                params_fingerprint=declaration_fingerprint(self.plan)[:512],
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

    def _ensure_job(self) -> Any:
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
                declaration_fingerprint=declaration_fingerprint(self.plan)[:512],
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
            declaration_fingerprint=declaration_fingerprint(self.plan)[:512],
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

        deadline = _Deadline(self.budget)
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
        return DurableRunResult(
            status="completed",
            pipeline_id=self.pipeline_id,
            job_id=self.job_id,
            invocation_id=self.invocation_id,
            committed_batches=committed,
            active_node_id=active_node_id,
            restart_safe=self.restart_safe,
        )


__all__ = [
    "CommittedDatasetReader",
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
