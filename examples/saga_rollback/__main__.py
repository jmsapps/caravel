from __future__ import annotations

import argparse
from typing import Sequence

from ..support import example_data_dir
from .pipeline import run_saga_pipeline


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the saga rollback example.")
    parser.add_argument(
        "--succeed",
        action="store_true",
        help="Complete the saga instead of triggering the rollback demonstration.",
    )
    args = parser.parse_args(argv)

    root = example_data_dir(__file__)
    run_saga_pipeline(
        state_root=root,
        state_path=root / "external_state.json",
        journal_path=root / "compensation_journal.json",
        fail=not args.succeed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
