from __future__ import annotations

from caravel.cli import make_cli

from .pipeline import build_multisource_pipeline

from ..support import example_output


def main() -> int:
    pipeline = build_multisource_pipeline()
    cli = make_cli(pipeline, source=pipeline.source, default_output=example_output(__file__))
    return cli()


if __name__ == "__main__":
    raise SystemExit(main())
