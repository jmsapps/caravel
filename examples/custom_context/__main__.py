from __future__ import annotations

from pathlib import Path

from .pipeline import run_custom_context_pipeline


def main() -> int:
    run_custom_context_pipeline(state_root=Path("data/custom_context_example"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
