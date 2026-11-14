"""A command-line entry point for one durable pipeline.

The CLI drives the same single execution model as :func:`caravel.run`. It
therefore requires the same explicit storage decision: a job that claims to be
resumable must say where its durable state lives.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence

from .options import DurabilityOptions
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
    default_output: Path | str | None = None,
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
    resolved_default = (
        Path(default_output)
        if default_output is not None
        else Path("data") / pipeline.name / "output"
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help=(f"Where generated output goes. Default: {resolved_default}. May be an fsspec URL."),
    )
    parser.add_argument(
        "--state",
        default=None,
        metavar="PATH",
        help=(
            "Recovery directory holding the pipeline's database. Reserved for "
            "Caravel; it is what lets a job resume. Default: '.caravel' beside "
            f"the output directory, or data/{pipeline.name}/.caravel when "
            "output is remote."
        ),
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help=(
            "Identity this run resumes under. Supplying it makes the run durable: "
            "committed steps are restored instead of recomputed."
        ),
    )
    parser.add_argument(
        "--commit-scope",
        default="step",
        choices=("pipeline", "stage", "step"),
        help="When a durable run commits. Default: step.",
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

        try:
            params = _parse_params(args.param)
        except ValueError as error:
            parser.error(str(error))

        output = args.output if args.output is not None else resolved_default
        state = args.state
        if state is None and "://" in str(output):
            # Remote output cannot host the recovery database; fall back to a
            # documented pipeline-scoped local directory.
            state = Path("data") / pipeline.name / ".caravel"

        if args.job_id:
            pipeline.durability_options = DurabilityOptions(
                schema_version="v1.0.0",
                job_id=args.job_id,
                commit_scope=args.commit_scope,
            )

        try:
            result = run_fn(
                pipeline,
                output=output,
                state=state,
                source=source if source is not None else pipeline.source,
                params=params or None,
                plugins=tuple(plugins) if plugins is not None else (),
            )
        except ValueError as error:
            parser.error(str(error))

        print(
            f"pipeline={result.pipeline_name} job_id={result.job_id} "
            f"executed={result.steps_executed} restored={result.steps_restored}"
        )
        print(f"output={result.output_root}")
        return 0

    return entrypoint


__all__ = ["make_cli"]
