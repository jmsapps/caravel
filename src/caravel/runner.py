"""The single execution entry point.

``run`` executes a pipeline's declared steps in order, each over the complete
value the previous step produced. Declaring ``durability_options`` on the
pipeline adds the recovery database, so a committed step is restored instead of
recomputed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from .pipeline import Pipeline


def run(
    pipeline: Pipeline,
    *,
    output: Path | str,
    source: Any = None,
    state: Path | str | None = None,
    params: Mapping[str, str] | None = None,
    context_factory: Any = None,
    keep_source_tag: bool = False,
    plugins: Sequence[Any] = (),
) -> Any:
    """Run ``pipeline`` and return a structured result.

    ``output`` is the root generated results are written under; it may be a
    filesystem path or an fsspec URL. ``state`` names the recovery directory a
    durable run uses, and defaults to ``.caravel`` beside the output root.

    Job identity and commit timing come from the pipeline's
    ``durability_options``.
    """
    from .durability.execution import Invocation
    from .durability.storage import OutputLocation

    resolved_source = source if source is not None else pipeline.source

    invocation = Invocation(
        pipeline=pipeline,
        output=OutputLocation(output),
        source=resolved_source,
        params=params,
        context_factory=context_factory,
        keep_source_tag=keep_source_tag,
        plugins=plugins,
        state=state,
    )
    return invocation.run()


__all__ = ["run"]
