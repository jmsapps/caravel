from __future__ import annotations

from pipeline.cli import make_cli

from .pipeline import build_multisource_pipeline


def main() -> int:
    pipeline = build_multisource_pipeline()
    cli = make_cli(pipeline)
    return cli()


if __name__ == "__main__":
    raise SystemExit(main())
