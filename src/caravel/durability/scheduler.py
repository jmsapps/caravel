"""Durable scheduling: plan compilation, discovery registration, and readiness.

Stages are declaration groups only. Scheduling reads the store's indexed
eligible-work query, so one partition can cross a stage boundary while another
is still in an earlier stage. There is no stage-wide completion check.

A step declaring ``input_scope="all"`` is a barrier: it becomes one work unit on
the reserved aggregate partition, with one dependency edge per upstream
partition. Its output is one stable partition for every subsequent step. That
reserved partition is private scheduling state: the public name of what the
barrier produced comes from its declaration, through
:mod:`caravel.durability.identity`. Scope is read from the declaration here,
before any source discovery or durable mutation, so an invalid declaration fails
having touched no state and no processor runs during planning.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal, Sequence

from ..branch import Branch
from ..datasets import JSONDataset
from ..pipeline import Pipeline, Stage, Step
from ..plan import LogicalPlan, _normalize_route_step, compile_pipeline
from ..retry import RetryOptions
from .identity import LogicalIdentityError, resolved_output_name, validate_output_name
from .sources import AGGREGATE_PARTITION_ID, SourcePartition
from .sqlite import SQLiteDurableStore
from .types import (
    DependencyEdge,
    DiscoveredPartition,
    DiscoveredWorkUnit,
    DurabilityError,
    InputScope,
    JobState,
    WorkUnitKey,
    WorkUnitState,
)

AGGREGATE_INPUT_REF = "aggregate://all-input"


class UnsupportedDurableDeclarationError(DurabilityError):
    """Raised when a declaration cannot execute under the durable contract."""


NodeKind = Literal["step", "branch", "merge"]


@dataclass(frozen=True)
class DurableNode:
    """One durable node in declaration order.

    A ``branch`` node decides a partition's route when that partition reaches
    it, because a predicate may read values earlier steps produced. A ``merge``
    node is partition-scoped: each partition travels exactly one route, so it
    merges when its own route's final step seals.
    """

    index: int
    node_id: str
    step: Step
    stage_index: int
    stage_name: str
    entry_index: int
    input_scope: InputScope
    kind: NodeKind = "step"
    branch: Branch | None = None
    routes: tuple[tuple[str, tuple[DurableNode, ...]], ...] = ()
    merge_node_id: str | None = None
    branch_node_id: str | None = None
    stage_retry: RetryOptions | None = None

    def route_chain(self, route_key: str) -> tuple[DurableNode, ...]:
        for key, chain in self.routes:
            if key == route_key:
                return chain
        raise KeyError(
            f"Branch '{self.node_id}' has no route '{route_key}'. A committed route "
            "that is absent from the current declaration fails closed."
        )


@dataclass(frozen=True)
class DurablePlan:
    """An ordered chain of durable step nodes compiled from a pipeline."""

    pipeline_name: str
    nodes: tuple[DurableNode, ...]
    logical: LogicalPlan

    def node_by_id(self, node_id: str) -> DurableNode:
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        raise KeyError(f"No durable node '{node_id}' in this plan.")

    @property
    def spine(self) -> tuple[DurableNode, ...]:
        """Nodes registered at discovery: everything except route steps.

        Route steps are registered when a partition's route is decided, so they
        are not part of the chain discovery walks.
        """
        return tuple(
            node for node in self.nodes if node.branch_node_id is None or node.kind == "merge"
        )

    def previous(self, node: DurableNode) -> DurableNode | None:
        """The node whose committed output feeds ``node``.

        Inside a route this is the previous route step, or the branch for the
        first one. On the spine it is the previous spine node.
        """
        if node.branch_node_id is not None and node.kind != "merge":
            branch = self.node_by_id(node.branch_node_id)
            for _, chain in branch.routes:
                for position, member in enumerate(chain):
                    if member.node_id == node.node_id:
                        return chain[position - 1] if position else branch
            raise KeyError(f"Route node '{node.node_id}' is not in its branch.")
        spine = self.spine
        for position, member in enumerate(spine):
            if member.node_id == node.node_id:
                return spine[position - 1] if position else None
        raise KeyError(f"Node '{node.node_id}' is not on the plan spine.")


def _compile_branch(
    branch: Branch,
    *,
    stage: Stage,
    entry_id: str,
    stage_index: int,
    entry_index: int,
    index: int,
) -> tuple[DurableNode, list[DurableNode], DurableNode, int]:
    """Compile one branch into a branch node, route chains, and a merge node."""
    if not isinstance(branch.routes, dict) or not branch.routes:
        raise UnsupportedDurableDeclarationError(
            f"Branch '{branch.name}' must declare at least one route."
        )

    merge_id = f"{entry_id}-merge"
    branch_index = index
    index += 1

    routes: list[tuple[str, tuple[DurableNode, ...]]] = []
    route_nodes: list[DurableNode] = []
    for route_number, route_key in enumerate(sorted(branch.routes), start=1):
        chain: list[DurableNode] = []
        for step_number, raw in enumerate(branch.routes[route_key], start=1):
            step = _normalize_route_step(raw, branch_name=branch.name)
            if step.input_scope == "all":
                raise UnsupportedDurableDeclarationError(
                    f"Branch '{branch.name}' route '{route_key}' step "
                    f"'{step.name}' declares input_scope=\"all\". A route step runs "
                    "per partition; place aggregation after convergence, in a step "
                    "that follows the merge."
                )
            node = DurableNode(
                index=index,
                node_id=f"{entry_id}-route-{route_number:03d}-step-{step_number:03d}",
                step=step,
                stage_index=stage_index,
                stage_name=stage.name,
                entry_index=entry_index,
                input_scope="partition",
                branch_node_id=entry_id,
                stage_retry=stage.retry,
            )
            chain.append(node)
            route_nodes.append(node)
            index += 1
        if not chain:
            raise UnsupportedDurableDeclarationError(
                f"Branch '{branch.name}' route '{route_key}' declares no steps."
            )
        routes.append((route_key, tuple(chain)))

    branch_node = DurableNode(
        index=branch_index,
        node_id=entry_id,
        step=Step(
            fn=_route_passthrough,
            output=JSONDataset(name=branch.name),
            name=branch.name,
        ),
        stage_index=stage_index,
        stage_name=stage.name,
        entry_index=entry_index,
        input_scope="partition",
        kind="branch",
        branch=branch,
        routes=tuple(routes),
        merge_node_id=merge_id,
    )
    merge_node = DurableNode(
        index=index,
        node_id=merge_id,
        step=Step(
            fn=_route_passthrough,
            output=JSONDataset(name=f"{branch.name}_merge"),
            name=f"{branch.name}_merge",
        ),
        stage_index=stage_index,
        stage_name=stage.name,
        entry_index=entry_index,
        input_scope="partition",
        kind="merge",
        branch_node_id=entry_id,
    )
    return branch_node, route_nodes, merge_node, index + 1


def _route_passthrough(inputs: Any, *, context: Any) -> Any:
    """Branch and merge nodes commit their partition's payload unchanged.

    These are scheduler-owned nodes, not user declarations, but they consume the
    same runtime inputs every processor receives: there is one execution path.
    """
    del context
    [partition] = inputs.partitions()
    return partition.load()


def compile_durable_plan(pipeline: Pipeline) -> DurablePlan:
    """Compile a pipeline into a durable node chain, rejecting what cannot resume.

    No processor runs here. Each step's declared ``input_scope`` supplies the
    per-partition versus aggregate topology the scheduler already uses, so
    nothing about a processor's behavior is discovered at planning time and
    nothing is inferred from its signature, name, or annotations.

    Compilation has no durable state transition, so a resume simply recompiles
    the declaration and validates the resulting fingerprint before any
    processor, source read, or mutation runs.
    """
    logical = compile_pipeline(pipeline)
    nodes: list[DurableNode] = []
    index = 0

    for stage_index, stage in enumerate(pipeline.stages, start=1):
        if stage.clean_dirs:
            raise UnsupportedDurableDeclarationError(
                f"Stage '{stage.name}' declares clean_dirs, which may delete committed "
                "batch output. Durable execution owns output lifetime; remove it."
            )
        for entry_index, entry in enumerate(stage.entries, start=1):
            entry_id = f"stage-{stage_index:03d}-entry-{entry_index:03d}"

            if isinstance(entry, Branch):
                branch_node, route_nodes, merge_node, index = _compile_branch(
                    entry,
                    stage=stage,
                    entry_id=entry_id,
                    stage_index=stage_index,
                    entry_index=entry_index,
                    index=index,
                )
                nodes.append(branch_node)
                nodes.extend(route_nodes)
                nodes.append(merge_node)
                continue

            if not isinstance(entry, Step):
                raise UnsupportedDurableDeclarationError(
                    f"Stage '{stage.name}' entry {entry_index} is a "
                    f"{type(entry).__name__}; expected a Step or a Branch."
                )
            scope: InputScope = entry.input_scope
            nodes.append(
                DurableNode(
                    index=index,
                    node_id=entry_id,
                    step=entry,
                    stage_index=stage_index,
                    stage_name=stage.name,
                    entry_index=entry_index,
                    input_scope=scope,
                    stage_retry=stage.retry,
                )
            )
            index += 1

    if not nodes:
        raise UnsupportedDurableDeclarationError(
            f"Pipeline '{pipeline.name}' declares no steps to execute."
        )

    _validate_output_names(nodes)
    return DurablePlan(pipeline_name=pipeline.name, nodes=tuple(nodes), logical=logical)


def _validate_output_names(nodes: Sequence[DurableNode]) -> None:
    """Reject output names that cannot address published output.

    A barrier's output, and everything downstream of it, is addressed by the
    declared dataset name rather than by the reserved aggregate execution ID.
    That name becomes a public identity, so it is validated here -- during
    compilation, before source discovery and before any durable mutation.
    """
    for node in nodes:
        try:
            validate_output_name(
                resolved_output_name(node),
                what=f"Output name for step '{node.step.name or node.node_id}'",
            )
        except LogicalIdentityError as error:
            raise UnsupportedDurableDeclarationError(str(error)) from error


def declaration_fingerprint(plan: DurablePlan) -> str:
    """Return a stable fingerprint of the declaration's durable identity.

    Version 3 covers declared scope and output identity. A job persisted under
    an earlier version describes a declaration contract this engine no longer
    executes, so it is rejected rather than reinterpreted.
    """
    declaration = {
        "pipeline": plan.pipeline_name,
        "nodes": [
            [
                node.node_id,
                node.step.name,
                node.step.revision,
                node.input_scope,
                node.step.batch_size,
                type(node.step.dataset).__name__,
                node.stage_name,
                resolved_output_name(node),
                getattr(node.step.dataset, "suffix", None),
            ]
            for node in plan.nodes
        ],
    }
    canonical = json.dumps(declaration, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return "schema=3|sha256=" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def register_plan_discovery(
    store: SQLiteDurableStore,
    job: JobState,
    plan: DurablePlan,
    partitions: Sequence[SourcePartition],
    *,
    seal: bool = True,
) -> JobState:
    """Register every partition, work unit, and readiness edge for a plan.

    The partition set collapses at a barrier: steps before the first all-input
    step run over source partitions, and from that barrier onward execution runs
    over the single aggregate partition.
    """
    if not partitions:
        discovered_partitions: list[DiscoveredPartition] = []
    else:
        discovered_partitions = []

    units_by_partition: dict[str, list[DiscoveredWorkUnit]] = {
        partition.partition_id: [] for partition in partitions
    }
    aggregate_units: list[DiscoveredWorkUnit] = []
    edges: list[DependencyEdge] = []

    current: tuple[str, ...] = tuple(partition.partition_id for partition in partitions)
    previous: DurableNode | None = None

    for node in plan.spine:
        unit_scope: InputScope = node.input_scope
        if unit_scope == "all":
            aggregate_units.append(
                DiscoveredWorkUnit(
                    node_id=node.node_id,
                    step_revision=node.step.revision,
                    params_fingerprint=declaration_fingerprint(plan),
                    input_scope="all",
                )
            )
            if previous is not None:
                for partition_id in current:
                    edges.append(
                        DependencyEdge(
                            node_id=node.node_id,
                            partition_id=AGGREGATE_PARTITION_ID,
                            depends_on_node_id=previous.node_id,
                            depends_on_partition_id=partition_id,
                        )
                    )
            current = (AGGREGATE_PARTITION_ID,)
        else:
            for partition_id in current:
                unit = DiscoveredWorkUnit(
                    node_id=node.node_id,
                    step_revision=node.step.revision,
                    params_fingerprint=declaration_fingerprint(plan),
                    input_scope="partition",
                )
                if partition_id == AGGREGATE_PARTITION_ID:
                    aggregate_units.append(unit)
                else:
                    units_by_partition[partition_id].append(unit)
                if previous is not None:
                    edges.append(
                        DependencyEdge(
                            node_id=node.node_id,
                            partition_id=partition_id,
                            depends_on_node_id=previous.node_id,
                            depends_on_partition_id=partition_id,
                        )
                    )
        previous = node

    for partition in partitions:
        discovered_partitions.append(
            DiscoveredPartition(
                partition_id=partition.partition_id,
                input_ref=partition.input_ref,
                input_version=partition.input_version,
                # Change lineage is preserved through discovery so the manifest
                # identifies the input versions and operations actually applied.
                entity_key=partition.entity_key,
                operation=partition.operation,
                source_revision=partition.source_revision,
                source_position=partition.source_position,
                work_units=tuple(units_by_partition[partition.partition_id]),
            )
        )

    if aggregate_units:
        discovered_partitions.append(
            DiscoveredPartition(
                partition_id=AGGREGATE_PARTITION_ID,
                input_ref=AGGREGATE_INPUT_REF,
                work_units=tuple(aggregate_units),
            )
        )

    return store.register_discovery(
        job.job_id,
        expected_generation=job.generation,
        partitions=discovered_partitions,
        extra_dependencies=edges,
        next_position="discovered",
        seal=seal,
    )


def next_eligible(
    store: SQLiteDurableStore, job_id: str, *, limit: int = 8
) -> WorkUnitState | None:
    """Return the next dependency-satisfied work unit, or None when none remain.

    The underlying query is index-backed and bounded by ``limit``; it never
    scans the job's full partition set.
    """
    eligible = store.eligible_work(job_id, limit=limit)
    return eligible[0] if eligible else None


def next_reclaimable(
    store: SQLiteDurableStore,
    job_id: str,
    *,
    limit: int = 8,
    include_live: bool = False,
) -> WorkUnitState | None:
    """Return the next unfinished work unit from a prior invocation, if any.

    ``include_live`` is valid only for a sole worker; see
    :meth:`SQLiteDurableStore.reclaimable_work`.
    """
    orphaned = store.reclaimable_work(job_id, limit=limit, include_live=include_live)
    return orphaned[0] if orphaned else None


def outstanding_work(store: SQLiteDurableStore, job_id: str, *, limit: int = 8) -> int:
    """Return how many eligible work units are currently waiting, up to ``limit``."""
    return len(store.eligible_work(job_id, limit=limit))


def work_unit_key(job_id: str, node_id: str, partition_id: str) -> WorkUnitKey:
    return WorkUnitKey(job_id=job_id, node_id=node_id, partition_id=partition_id)


__all__ = [
    "AGGREGATE_INPUT_REF",
    "DurableNode",
    "NodeKind",
    "DurablePlan",
    "UnsupportedDurableDeclarationError",
    "compile_durable_plan",
    "declaration_fingerprint",
    "next_eligible",
    "next_reclaimable",
    "outstanding_work",
    "register_plan_discovery",
    "work_unit_key",
]
