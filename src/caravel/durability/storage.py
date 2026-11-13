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
ARTIFACTS_DIRNAME = "artifacts"


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
        _temporary: Any = None,
    ) -> None:
        self.progress = progress
        self.output = output
        self.restart_safe = restart_safe
        self.root = root
        self._temporary = _temporary

    @classmethod
    def at(
        cls,
        root: str | Path,
        *,
        artifacts_root: str | Path | None = None,
        storage_options: Mapping[str, Any] | None = None,
        timeout: float = 5.0,
    ) -> DurableStorage:
        """Durable state under ``root``, surviving invocations.

        ``root`` holds the authoritative progress store and must be a local
        filesystem path that outlives the process. It is the component that has
        to supply transactions and fencing, so network filesystems and
        ephemeral disk are not supported for it.

        ``artifacts_root`` locates committed output. It is configured and
        validated independently and may be any fsspec target, including object
        storage. Omit it to keep artifacts beside the state.
        """
        base = Path(root)
        base.mkdir(parents=True, exist_ok=True)
        artifacts: str | Path = (
            artifacts_root if artifacts_root is not None else base / ARTIFACTS_DIRNAME
        )
        return cls(
            progress=SQLiteDurableStore(base / STATE_FILENAME, timeout=timeout),
            output=AttemptOutputStore(artifacts, storage_options=storage_options),
            restart_safe=True,
            root=base,
        )

    @classmethod
    def ephemeral(cls, *, root: str | Path | None = None) -> DurableStorage:
        """State that does **not** survive the process.

        This is an explicit opt-out. A run using it reports
        ``restart_safe == False``, because nothing here can honour a resume.
        """
        temporary = None
        if root is None:
            temporary = tempfile.TemporaryDirectory(prefix="caravel-ephemeral-")
            base = Path(temporary.name)
        else:
            base = Path(root)
            base.mkdir(parents=True, exist_ok=True)
        return cls(
            progress=SQLiteDurableStore(base / STATE_FILENAME),
            output=AttemptOutputStore(base / ARTIFACTS_DIRNAME),
            restart_safe=False,
            root=base,
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
        return self.progress.invalidate_node(job_id, node.node_id, partitions=partitions)

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


__all__ = ["ARTIFACTS_DIRNAME", "STATE_FILENAME", "DurableStorage", "JobResults"]
