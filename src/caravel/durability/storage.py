"""Explicit durable storage configuration for one execution model.

Durability is foundational, so storage is never selected silently. A caller
chooses :meth:`DurableStorage.at` for state that survives invocations, or
:meth:`DurableStorage.ephemeral` to opt out explicitly. Ephemeral storage
reports ``restart_safe == False`` and that honesty propagates into the run
result: nothing claims restart safety it cannot provide.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..types import Dataset
from .commits import BatchPublisher
from .feed import BatchFeed
from .output import AttemptOutputStore
from .sqlite import SQLiteDurableStore
from .types import WorkUnitKey, validate_identifier

STATE_FILENAME = "state.sqlite3"
ARTIFACTS_DIRNAME = "attempts"
INTERNAL_DIRNAME = ".caravel"
LEGACY_INTERNAL_DIRNAME = "_internal"
OUTPUT_DIRNAME = "output"


class LegacyStateError(Exception):
    """Raised when durable state sits in the superseded ``_internal`` directory.

    Reserved execution storage moved from ``_internal/`` to ``.caravel/``.
    Existing state is never silently abandoned: a job whose progress store
    lives in the old location would otherwise look like a brand new job and
    reprocess everything.
    """


def _is_remote(location: str | Path) -> bool:
    return "://" in str(location)


def _as_location(value: str | Path) -> str | Path:
    """Keep an fsspec URL as text; make a filesystem path a Path."""
    return str(value) if _is_remote(value) else Path(value)


def _resolve_state_dir(output: str | Path, state: str | Path | None) -> Path:
    """Return the reserved recovery directory, named exactly as given.

    ``state`` names the directory itself: no further ``.caravel`` is created
    beneath it.
    """
    if state is not None:
        if _is_remote(state):
            raise ValueError(
                f"Recovery storage must be a local filesystem path, got '{state}'. "
                "It supplies transactions and fencing, which object storage does not."
            )
        reserved = Path(state)
    elif _is_remote(output):
        raise ValueError(
            f"Output '{output}' is remote, so recovery storage cannot sit beside it. "
            "Pass state=<local path>, or use the pipeline-scoped default "
            "'data/<pipeline-name>/.caravel'."
        )
    else:
        reserved = Path(output).parent / INTERNAL_DIRNAME

    legacy = reserved.parent / LEGACY_INTERNAL_DIRNAME
    if not (reserved / STATE_FILENAME).exists() and (legacy / STATE_FILENAME).exists():
        raise LegacyStateError(
            f"Durable state exists under '{legacy}', which has been renamed to "
            f"'{reserved}'. Move it to keep the job resumable:\n"
            f"    mv {legacy} {reserved}\n"
            "Starting fresh instead would reprocess everything already committed."
        )
    nested = reserved / INTERNAL_DIRNAME
    if not (reserved / STATE_FILENAME).exists() and (nested / STATE_FILENAME).exists():
        raise LegacyStateError(
            f"'{reserved}' contains '{nested}', so it was a *parent* of the recovery "
            "directory under the previous meaning of --state/state=. It now names the "
            f"recovery directory itself. Point it at '{nested}' to find existing jobs."
        )
    reserved.mkdir(parents=True, exist_ok=True)
    return reserved


def _options_for(
    output: str | Path,
    explicit: Mapping[str, Any] | None,
    artifact_options: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Backend options for the *published* tree.

    Artifact options describe the attempt store, which may be remote while the
    published tree is local. Reusing them for a local path would hand a cloud
    SDK's credentials to the local filesystem, so they carry over only when the
    output is itself remote.
    """
    if explicit is not None:
        return dict(explicit)
    if artifact_options and _is_remote(output):
        return dict(artifact_options)
    return None


def _reject_overlap(state: Path, output: str | Path, artifacts: str | Path | None) -> None:
    """Refuse configurations where cleanup could destroy authoritative state."""
    if _is_remote(output):
        return
    reserved = state.resolve()
    published = Path(output).resolve()
    if reserved == published:
        raise ValueError(
            f"Recovery storage and output are the same directory ('{published}'). "
            "Publishing would overwrite authoritative state."
        )
    if reserved.is_relative_to(published):
        raise ValueError(
            f"Recovery storage '{reserved}' is inside the output directory "
            f"'{published}'. Rebuilding or cleaning output would destroy the "
            "progress store and attempt artifacts."
        )
    if published.is_relative_to(reserved):
        raise ValueError(
            f"Output '{published}' is inside recovery storage '{reserved}'. "
            "Published results must not live in reserved execution storage."
        )
    if artifacts is not None and not _is_remote(artifacts):
        resolved_artifacts = Path(artifacts).resolve()
        if resolved_artifacts.is_relative_to(published) or published.is_relative_to(
            resolved_artifacts
        ):
            raise ValueError(
                f"Attempt artifacts '{resolved_artifacts}' overlap the output "
                f"directory '{published}'. Publication could delete committed "
                "artifacts."
            )


class JobResults:
    """Read one job's committed output through its declared datasets."""

    def __init__(
        self,
        storage: DurableStorage,
        plan: Any,
        job_id: str,
    ) -> None:
        self._storage = storage
        self._plan = plan
        self.job_id = validate_identifier("job_id", job_id)

    def _node_for(self, step: str) -> Any:
        for node in self._plan.nodes:
            if node.step.name == step or node.node_id == step:
                return node
        raise KeyError(
            f"No step named '{step}' in pipeline '{self._plan.pipeline_name}'. "
            f"Available: {sorted(node.step.name or node.node_id for node in self._plan.nodes)}"
        )

    def of(self, step: str, *, dataset: Dataset | None = None) -> dict[str, Any]:
        """Return committed output for a step, keyed by execution partition.

        A partition that committed one batch yields that payload directly; a
        batched partition yields the tuple of its committed batches in order.
        """
        node = self._node_for(step)
        declared = dataset if dataset is not None else node.step.dataset
        publisher = BatchPublisher(self._storage.progress, self._storage.output)
        collected: dict[str, Any] = {}
        for partition_id in self._storage.progress.work_unit_partitions(self.job_id, node.node_id):
            key = WorkUnitKey(self.job_id, node.node_id, partition_id)
            payloads = publisher.load_work_unit(key, dataset=declared)
            if not payloads:
                continue
            collected[partition_id] = payloads[0] if len(payloads) == 1 else payloads
        return collected

    def published(self, step: str, *, pipeline_id: str | None = None) -> dict[str, list[str]]:
        """Where a step's current output is published, keyed by partition.

        Published locations are generation-specific, so this is the supported
        way to find the current files: never infer them from a directory
        listing, which may still contain superseded generations.
        """
        node = self._node_for(step)
        projection = self._storage.publisher(pipeline_id or self._plan.pipeline_name)
        if projection is None:
            return {}
        located: dict[str, list[str]] = {}
        for partition_id in self._storage.progress.work_unit_partitions(self.job_id, node.node_id):
            key = WorkUnitKey(self.job_id, node.node_id, partition_id)
            unit = self._storage.progress.load_work_unit(key)
            if unit is None or unit.status != "sealed":
                continue
            job_dir = projection.layout(self.job_id).job_dir()
            located[partition_id] = [
                f"{job_dir}/{item.relative}"
                for item in projection.planned_files(node, key, unit.output_generation)
            ]
        return located

    def feed(self, step: str, *, pipeline_id: str | None = None) -> BatchFeed:
        """Return the committed batch feed for a step's output node."""
        node = self._node_for(step)
        return BatchFeed(
            self._storage.progress,
            pipeline_id=pipeline_id or self._plan.pipeline_name,
            job_id=self.job_id,
            node_id=node.node_id,
            output=self._storage.output,
        )


class DurableStorage:
    """Where durable execution keeps its authoritative state and its artifacts.

    Construct through :meth:`at` or :meth:`ephemeral` rather than directly, so
    the restart-safety claim is always an explicit choice.
    """

    def __init__(
        self,
        progress: SQLiteDurableStore,
        output: AttemptOutputStore,
        *,
        restart_safe: bool,
        root: Path | None = None,
        output_root: str | Path | None = None,
        storage_options: Mapping[str, Any] | None = None,
        keep_superseded: int | None = None,
        _temporary: Any = None,
    ) -> None:
        self.progress = progress
        self.output = output
        self.restart_safe = restart_safe
        self.root = root
        self.output_root = output_root
        self.storage_options = dict(storage_options) if storage_options else None
        self.keep_superseded = keep_superseded
        self._temporary = _temporary

    @property
    def publishes(self) -> bool:
        """Whether this storage materializes a readable output tree."""
        return self.output_root is not None

    def publisher(self, pipeline_id: str) -> Any:
        """The projection that materializes committed results, or None."""
        if self.output_root is None:
            return None
        from .publish import OutputPublisher

        return OutputPublisher(
            self.progress,
            self.output,
            root=self.output_root,
            pipeline_id=pipeline_id,
            storage_options=self.storage_options,
            keep_superseded=self.keep_superseded,
        )

    @classmethod
    def at(
        cls,
        output: str | Path,
        *,
        state: str | Path | None = None,
        artifacts_root: str | Path | None = None,
        storage_options: Mapping[str, Any] | None = None,
        output_storage_options: Mapping[str, Any] | None = None,
        keep_superseded: int | None = None,
        timeout: float = 5.0,
    ) -> DurableStorage:
        """Durable execution publishing to ``output``.

        ``output`` is where readable results go, and it is the only required
        argument: execution is durable by default.

        ``state`` is the recovery directory itself -- the progress store and
        attempt artifacts -- and it defaults to ``.caravel`` beside ``output``.
        It is reserved: nothing but Caravel may write there, and deleting it
        loses the ability to resume. It must be a local filesystem path that
        outlives the process, because it supplies transactions and fencing.

        ``output`` may be any fsspec target. A remote one cannot host the
        progress store, so pass ``state`` explicitly for it; callers that know
        the pipeline name should use the pipeline-scoped local default
        ``data/<pipeline-name>/.caravel``.
        """
        published = _as_location(output)
        reserved = _resolve_state_dir(published, state)
        _reject_overlap(reserved, published, artifacts_root)
        artifacts: str | Path = (
            artifacts_root if artifacts_root is not None else reserved / ARTIFACTS_DIRNAME
        )
        return cls(
            progress=SQLiteDurableStore(reserved / STATE_FILENAME, timeout=timeout),
            output=AttemptOutputStore(artifacts, storage_options=storage_options),
            restart_safe=True,
            root=reserved,
            output_root=published,
            storage_options=_options_for(published, output_storage_options, storage_options),
            keep_superseded=keep_superseded,
        )

    @classmethod
    def ephemeral(
        cls,
        output: str | Path,
        *,
        artifacts_root: str | Path | None = None,
        storage_options: Mapping[str, Any] | None = None,
        output_storage_options: Mapping[str, Any] | None = None,
        keep_superseded: int | None = None,
    ) -> DurableStorage:
        """Throwaway recovery storage, with published results kept.

        Only the recovery directory is temporary. Published output stays at
        ``output``, so switching between durable and ephemeral execution never
        moves where results land, and closing the storage never deletes them.

        A run using this reports ``restart_safe == False``: nothing here can
        honour a resume.
        """
        published = _as_location(output)
        temporary = tempfile.TemporaryDirectory(prefix="caravel-state-")
        reserved = Path(temporary.name)
        _reject_overlap(reserved, published, artifacts_root)
        artifacts: str | Path = (
            artifacts_root if artifacts_root is not None else reserved / ARTIFACTS_DIRNAME
        )
        return cls(
            progress=SQLiteDurableStore(reserved / STATE_FILENAME),
            output=AttemptOutputStore(artifacts, storage_options=storage_options),
            restart_safe=False,
            root=reserved,
            output_root=published,
            storage_options=_options_for(published, output_storage_options, storage_options),
            keep_superseded=keep_superseded,
            _temporary=temporary,
        )

    def results(self, pipeline: Any, *, job_id: str) -> JobResults:
        """Read a completed or in-progress job's committed output."""
        from .scheduler import compile_durable_plan

        plan = pipeline if hasattr(pipeline, "nodes") else compile_durable_plan(pipeline)
        return JobResults(self, plan, job_id)

    def invalidate(
        self,
        pipeline: Any,
        *,
        job_id: str,
        step: str,
        partitions: Sequence[str] | None = None,
    ) -> tuple[WorkUnitKey, ...]:
        """Mark a step's output stale so the next invocation recomputes it.

        Everything derived from that step is invalidated with it: leaving
        dependents sealed against input that no longer exists would let the job
        report complete while its manifest described a dataset no single
        execution produced.

        Committed artifacts are not deleted. They are superseded, so a consumer
        reading the feed can tell a replacement from new data. Re-invoke
        ``run()`` with the same ``job_id`` afterwards to produce the new
        generation.
        """
        plan = self.results(pipeline, job_id=job_id)._plan
        node = JobResults(self, plan, job_id)._node_for(step)
        projection = self.publisher(plan.pipeline_name)

        # Removing the advisory marker on both sides reduces staleness.
        # Readers establish currency through transactional manifest validation.
        if projection is not None:
            projection.remove_job_manifest(job_id)

        invalidated_generations: dict[WorkUnitKey, int] = {}
        invalidated = self.progress.invalidate_node(
            job_id,
            node.node_id,
            partitions=partitions,
            invalidated_generations=invalidated_generations,
        )

        if projection is not None:
            projection.remove_job_manifest(job_id)
            by_id = {item.node_id: item for item in plan.nodes}
            for key in invalidated:
                target = by_id.get(key.node_id)
                if target is not None:
                    projection.unpublish_unit(
                        target, key, through_generation=invalidated_generations[key]
                    )
        return invalidated

    def rebuild_output(self, pipeline: Any, *, job_id: str) -> int:
        """Rebuild the published tree from committed metadata.

        ``reconcile`` republishes only what the store believes is pending, so
        it cannot repair output that was deleted or altered behind its back.
        This is the supported operation for that case.
        """
        projection = self.publisher(self.results(pipeline, job_id=job_id)._plan.pipeline_name)
        if projection is None:
            return 0
        plan = self.results(pipeline, job_id=job_id)._plan
        return int(projection.rebuild(job_id, plan.nodes))

    def close(self) -> None:
        self.progress.close()
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None

    def __enter__(self) -> DurableStorage:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        kind = "at" if self.restart_safe else "ephemeral"
        return f"DurableStorage.{kind}({self.root!s}) restart_safe={self.restart_safe}"


__all__ = [
    "ARTIFACTS_DIRNAME",
    "INTERNAL_DIRNAME",
    "LEGACY_INTERNAL_DIRNAME",
    "LegacyStateError",
    "OUTPUT_DIRNAME",
    "STATE_FILENAME",
    "DurableStorage",
    "JobResults",
]
