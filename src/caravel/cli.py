"""A command-line entry point for one durable pipeline.

The CLI drives the same single execution model as :func:`caravel.run`. It
therefore requires the same explicit storage decision: a job that claims to be
resumable must say where its durable state lives.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence

from .durability.storage import DurableStorage, LegacyStateError
from .runner import run
from .viz import to_mermaid

RunFn = Callable[..., Any]
MermaidFn = Callable[[Any], str]


class CliEntrypoint(Protocol):
    """Callable returned by `make_cli` — argv defaults to `sys.argv[1:]`."""

    def __call__(self, argv: Sequence[str] | None = None) -> int: ...


def _parse_params(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError("Invalid --param value. Expected key=value.")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError("Invalid --param value. Key cannot be empty.")
        parsed[key] = value
    return parsed


def make_cli(
    pipeline: Any,
    *,
    run_fn: RunFn = run,
    mermaid_fn: MermaidFn | None = to_mermaid,
    plugins: Sequence[Any] | None = None,
    source: Any = None,
) -> CliEntrypoint:
    """Create a CLI entrypoint callable for a pipeline declaration.

    Plugins are application-owned composition: the entry point that builds the
    CLI passes explicit observer and run-guard instances, forwarded to the run.
    """

    parser = argparse.ArgumentParser(
        prog=pipeline.name,
        description=f"Run the '{pipeline.name}' pipeline.",
    )
    default_output = Path("data") / pipeline.name / "output"
    parser.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help=(f"Where published results go. Default: {default_output}. May be an fsspec URL."),
    )
    parser.add_argument(
        "--state",
        default=None,
        metavar="PATH",
        help=(
            "Recovery storage directory: the progress store and attempt "
            "artifacts. Reserved for Caravel; deleting it loses the ability to "
            "resume. Default: '.caravel' beside the output directory, or "
            f"data/{pipeline.name}/.caravel when output is remote."
        ),
    )
    parser.add_argument(
        "--ephemeral",
        action="store_true",
        help=(
            "Use throwaway recovery storage. Published results still go to the "
            "configured output; nothing resumes and the run reports "
            "restart_safe=false."
        ),
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help="Identity of this bounded run. Required unless --ephemeral.",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Run-scoped parameter. Repeatable.",
    )
    parser.add_argument(
        "--graph",
        action="store_true",
        help="Print the pipeline as a Mermaid graph and exit without running.",
    )

    def entrypoint(argv: Sequence[str] | None = None) -> int:
        args = parser.parse_args(argv)

        if args.graph:
            if mermaid_fn is None:
                parser.error("Graph rendering is not available.")
            print(mermaid_fn(pipeline))
            return 0

        if args.ephemeral and args.state:
            parser.error(
                "--ephemeral uses throwaway recovery storage, so --state has no effect. Choose one."
            )
        if not args.ephemeral and not args.job_id:
            parser.error(
                "--job-id is required: a durable run needs a stable identity to "
                "resume. Use --ephemeral for a throwaway run."
            )

        try:
            params = _parse_params(args.param)
        except ValueError as error:
            parser.error(str(error))

        output = args.output if args.output is not None else default_output
        try:
            if args.ephemeral:
                storage = DurableStorage.ephemeral(output)
            else:
                state = args.state
                if state is None and "://" in str(output):
                    # Remote output cannot host the progress store; fall back to
                    # a documented pipeline-scoped local directory.
                    state = Path("data") / pipeline.name / ".caravel"
                storage = DurableStorage.at(output, state=state)
        except (ValueError, LegacyStateError) as error:
            parser.error(str(error))

        result = run_fn(
            pipeline,
            job_id=args.job_id,
            source=source if source is not None else pipeline.source,
            storage=storage,
            params=params or None,
            plugins=tuple(plugins) if plugins is not None else (),
        )
        print(
            f"status={result.status} job_id={result.job_id} "
            f"batches={result.committed_batches} restart_safe={result.restart_safe}"
        )
        print(f"output={storage.output_root}")
        return 0 if result.status in ("completed", "caught_up", "yielded") else 1

    return entrypoint


__all__ = ["make_cli"]
