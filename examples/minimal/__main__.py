"""CLI entry point for the minimal example pipeline.

Run from the repo root, for example:

    .\\.venv\\Scripts\\python.exe -m examples.minimal --run-root src\\poc\\data\\minimal_example\\smoke_run

See README.md in this folder for full usage and debug flags.
"""

from __future__ import annotations

from pipeline.cli import make_cli

from .pipeline import build_minimal_pipeline


def main() -> int:
    pipeline = build_minimal_pipeline()
    cli = make_cli(pipeline)
    return cli()


if __name__ == "__main__":
    raise SystemExit(main())
