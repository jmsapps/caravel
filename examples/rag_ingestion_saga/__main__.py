from __future__ import annotations

import argparse
from typing import Sequence

from ..support import example_data_dir
from .pipeline import run_rag_ingestion


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the conceptual RAG ingestion saga.")
    parser.add_argument("--succeed", action="store_true", help="Commit instead of compensating.")
    args = parser.parse_args(argv)
    root = example_data_dir(__file__)
    run_rag_ingestion(
        state_root=root,
        workspace_root=root / "external_workspace",
        fail=not args.succeed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
