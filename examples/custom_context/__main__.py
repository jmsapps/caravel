from __future__ import annotations

from pathlib import Path

from .pipeline import run_custom_context_pipeline


def main() -> int:
    run_custom_context_pipeline(run_root=Path("data/output/custom_context"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
