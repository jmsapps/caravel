"""A command-line entry point for one durable pipeline.

The CLI drives the same single execution model as :func:`caravel.run`. It
therefore requires the same explicit storage decision: a job that claims to be
resumable must say where its durable state lives.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence

from .durability.storage import DurableStorage
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
    parser.add_argument(
        "--state",
        default=None,
        help=("Directory for durable state and artifacts. Required unless --ephemeral is given."),
    )
    parser.add_argument(
        "--ephemeral",
        action="store_true",
        help=(
            "Run without durable state. Nothing resumes, and the run reports restart_safe=false."
        ),
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help="Identity of this bounded run. Required with --state.",
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

        if args.state and args.ephemeral:
            parser.error("Choose either --state or --ephemeral, not both.")
        if not args.state and not args.ephemeral:
            parser.error(
                "Durable execution needs a storage decision: pass --state DIR to "
                "keep state across invocations, or --ephemeral to opt out."
            )

        try:
            params = _parse_params(args.param)
        except ValueError as error:
            parser.error(str(error))

        storage = (
            DurableStorage.ephemeral() if args.ephemeral else DurableStorage.at(Path(args.state))
        )
        if storage.restart_safe and not args.job_id:
            parser.error("--job-id is required with --state: a resume needs an identity.")

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
        return 0 if result.status in ("completed", "caught_up", "yielded") else 1

    return entrypoint


__all__ = ["make_cli"]
