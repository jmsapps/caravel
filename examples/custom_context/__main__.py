from __future__ import annotations

from ..support import example_data_dir
from .pipeline import run_custom_context_pipeline


def main() -> int:
    run_custom_context_pipeline(state_root=example_data_dir(__file__))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
