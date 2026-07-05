from __future__ import annotations

import os

from caravel.cli import make_cli

from examples.fsspec_minimal.pipeline import (
    _storage_options_for_url,
    build_fsspec_pipeline,
)

from .profile import build_production_plugins


def main() -> int:
    metadata_root = os.getenv("CARAVEL_METADATA_ROOT", "").strip()
    if not metadata_root:
        raise SystemExit("CARAVEL_METADATA_ROOT is required for the production profile")

    pipeline = build_fsspec_pipeline()
    plugins = build_production_plugins(
        metadata_root,
        storage_options=_storage_options_for_url(metadata_root),
    )
    return make_cli(pipeline, plugins=plugins)()


if __name__ == "__main__":
    raise SystemExit(main())
