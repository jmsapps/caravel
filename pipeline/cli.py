"""CLI helper for declared PoC pipelines."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence

from .runner import run
from .types import MissingPriorOutputError
from .viz import to_mermaid

RunFn = Callable[..., Path]
MermaidFn = Callable[[Any], str]


class CliEntrypoint(Protocol):
    """Callable returned by `make_cli` — argv defaults to `sys.argv[1:]`."""

    def __call__(self, argv: Sequence[str] | None = None) -> int: ...


def _coerce_selector(value: str | None) -> str | int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return value


def _parse_params(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid --param value '{item}'. Expected key=value.")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --param value '{item}'. Key cannot be empty.")
        parsed[key] = value
    return parsed


def make_cli(
    pipeline: Any,
    *,
    run_fn: RunFn = run,
    mermaid_fn: MermaidFn | None = to_mermaid,
) -> CliEntrypoint:
    """Create a CLI entrypoint callable for a pipeline declaration."""

    parser = argparse.ArgumentParser(
        prog=pipeline.name,
        description=f"Run pipeline '{pipeline.name}' or emit Mermaid graph.",
    )
    parser.add_argument("--run-root", dest="run_root", help="Output run root path override.")
    parser.add_argument("--stage", dest="stage", help="Stage selector (name or 1-based index).")
    parser.add_argument("--step", dest="step", help="Step selector (name or 1-based index).")
    parser.add_argument(
        "--param",
        dest="params",
        action="append",
        default=[],
        metavar="key=value",
        help="Custom runtime parameter forwarded to step context; repeatable.",
    )
    parser.add_argument(
        "--keep-source-tag",
        dest="keep_source_tag",
        action="store_true",
        help="Preserve __source__ field in persisted output payloads.",
    )
    parser.add_argument(
        "--mermaid",
        dest="mermaid",
        help="Emit Mermaid diagram to output file path without executing pipeline.",
    )

    def cli_entrypoint(argv: Sequence[str] | None = None) -> int:
        args = parser.parse_args(list(argv) if argv is not None else None)

        has_execution_flags = any(
            [
                args.run_root is not None,
                args.stage is not None,
                args.step is not None,
                bool(args.params),
                bool(args.keep_source_tag),
            ]
        )

        if args.mermaid is not None:
            if has_execution_flags:
                parser.error("--mermaid cannot be combined with execution options.")
            if mermaid_fn is None:
                parser.error("--mermaid requires an available Mermaid renderer.")

            content = mermaid_fn(pipeline)
            Path(args.mermaid).write_text(content, encoding="utf-8")
            return 0

        only_stage = _coerce_selector(args.stage)
        only_step = _coerce_selector(args.step)
        run_root = Path(args.run_root) if args.run_root is not None else None

        try:
            params = _parse_params(args.params)
        except ValueError as exc:
            parser.error(str(exc))

        try:
            run_fn(
                pipeline,
                run_root=run_root,
                only_stage=only_stage,
                only_step=only_step,
                params=params,
                keep_source_tag=bool(args.keep_source_tag),
            )
        except (ValueError, MissingPriorOutputError) as exc:
            parser.error(str(exc))

        return 0

    return cli_entrypoint


__all__ = ["make_cli"]
