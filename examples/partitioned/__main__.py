"""CLI entry point for the partitioned ST-09b example pipeline."""

from __future__ import annotations

from pipeline.cli import make_cli

from .pipeline import build_partitioned_pipeline


def main() -> int:
    pipeline = build_partitioned_pipeline()
    cli = make_cli(pipeline)
    return cli()


if __name__ == "__main__":
    raise SystemExit(main())
