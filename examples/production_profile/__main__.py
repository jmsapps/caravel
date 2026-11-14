from __future__ import annotations

import os

from caravel.cli import make_cli

from examples.fsspec_minimal.pipeline import build_fsspec_pipeline

from .profile import build_production_plugins, storage_options_for_url


def main() -> int:
    metadata_root = os.getenv("CARAVEL_METADATA_ROOT", "").strip()
    if not metadata_root:
        raise SystemExit("CARAVEL_METADATA_ROOT is required for the production profile")

    # Input and evidence may live in different places with different
    # credentials, so each resolves its own options from the environment.
    input_url = os.getenv("CARAVEL_INPUT_URL", "").strip() or None
    pipeline = build_fsspec_pipeline(
        input_path=input_url,
        storage_options=storage_options_for_url(input_url),
    )
    plugins = build_production_plugins(
        metadata_root,
        storage_options=storage_options_for_url(metadata_root),
    )
    return make_cli(pipeline, source=pipeline.source, plugins=plugins)()


if __name__ == "__main__":
    raise SystemExit(main())
