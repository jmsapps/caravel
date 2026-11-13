"""The single execution entry point.

Caravel has one execution model: durable partition execution. There is no
second engine and no switch that disables durability. Batching, retries,
storage, and concurrency are configurations of this one model.

The legacy single-pass runner, with its bound-plan nodes, stage-directory
output layout, and node-level checkpoint plugins, has been retired. Its
responsibilities now belong to the authoritative progress store.
"""

from __future__ import annotations

import uuid
from typing import Any, Mapping, Sequence

from .pipeline import Pipeline


def run(
    pipeline: Pipeline,
    *,
    job_id: str | None = None,
    source: Any = None,
    storage: Any = None,
    pipeline_id: str | None = None,
    budget: Any = None,
    policy: Any = None,
    worker_id: str | None = None,
    params: Mapping[str, str] | None = None,
    context_factory: Any = None,
    keep_source_tag: bool = False,
    lineage: str = "default",
    snapshot_id: str | None = None,
    plugins: Sequence[Any] = (),
) -> Any:
    """Start or continue one bounded durable run and return a structured status.

    This is the only execution entry point. Durability is foundational, so
    there is no switch that turns it off: batching, retries, storage, and
    concurrency all configure this same model.

    ``storage`` is required and never defaulted. Choose
    ``DurableStorage.at(path)`` for state that survives invocations, or
    ``DurableStorage.ephemeral()`` to opt out explicitly; an ephemeral run
    reports ``restart_safe == False`` rather than implying a resume it cannot
    honour.

    ``pipeline_id`` defaults to the pipeline's name. ``job_id`` identifies one
    bounded run and is required for durable storage, because a resume has no
    meaning without it; an ephemeral run may omit it.
    """
    from .durability.execution import DurableInvocation
    from .durability.scheduler import compile_durable_plan
    from .durability.sources import as_source
    from .durability.storage import DurableStorage

    if storage is None:
        raise ValueError(
            "run() requires explicit storage. Use DurableStorage.at(path) for state "
            "that survives invocations, or DurableStorage.ephemeral() to opt out of "
            "restart safety deliberately."
        )
    if not isinstance(storage, DurableStorage):
        raise TypeError(f"run(storage=...) expects a DurableStorage, got {type(storage).__name__}.")

    if job_id is None:
        if storage.restart_safe:
            raise ValueError(
                "Durable storage requires an explicit job_id: a resume cannot be "
                "honoured without a stable identity for this bounded run."
            )
        job_id = f"ephemeral-{uuid.uuid4().hex}"

    resolved_source = as_source(source if source is not None else pipeline.source)

    invocation = DurableInvocation(
        plan=compile_durable_plan(pipeline),
        store=storage.progress,
        output=storage.output,
        source=resolved_source,
        pipeline_id=pipeline_id or pipeline.name,
        job_id=job_id,
        budget=budget,
        policy=policy,
        worker_id=worker_id,
        params=params,
        context_factory=context_factory,
        keep_source_tag=keep_source_tag,
        snapshot_id=snapshot_id,
        lineage=lineage,
        restart_safe=storage.restart_safe,
        plugins=plugins,
        storage_root=str(storage.root or ""),
    )
    return invocation.run()


__all__ = ["run"]
