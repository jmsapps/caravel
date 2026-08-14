from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .pipeline import run_rag_ingestion


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the conceptual RAG ingestion saga.")
    parser.add_argument("--succeed", action="store_true", help="Commit instead of compensating.")
    args = parser.parse_args(argv)
    root = Path("data/output/rag_ingestion_saga")
    run_rag_ingestion(
        run_root=root / "pipeline",
        workspace_root=root / "external_workspace",
        fail=not args.succeed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
