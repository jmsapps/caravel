from __future__ import annotations

from caravel.cli import make_cli

from .pipeline import build_fsspec_partitioned_pipeline


def main() -> int:
    pipeline = build_fsspec_partitioned_pipeline()
    cli = make_cli(pipeline)
    return cli()


if __name__ == "__main__":
    raise SystemExit(main())
