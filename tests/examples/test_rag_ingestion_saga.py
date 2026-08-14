import json
import sys
from pathlib import Path
from typing import Any, cast

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from examples.rag_ingestion_saga.pipeline import run_rag_ingestion


def _read(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def test_failure_restores_previous_chunks_index_and_version(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"

    with pytest.raises(RuntimeError, match="chunk promotion"):
        run_rag_ingestion(run_root=tmp_path / "runs", workspace_root=workspace, fail=True)

    assert _read(workspace / "version.json") == {"version": "v1"}
    assert _read(workspace / "current" / "chunk-001.json") == {"text": "previous chunks"}
    assert _read(workspace / "index" / "documents.json") == {"version": "v1", "chunks": 1}
    assert _read(workspace / "saga_journal.json")["status"] == "compensated"
    assert not (workspace / "staging").exists()


def test_success_commits_new_chunks_index_and_version(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"

    run_rag_ingestion(run_root=tmp_path / "runs", workspace_root=workspace)

    assert _read(workspace / "version.json") == {"version": "v2"}
    assert _read(workspace / "index" / "documents.json") == {"version": "v2", "chunks": 3}
    assert len(list((workspace / "current").glob("chunk-*.json"))) == 3
    assert _read(workspace / "saga_journal.json")["status"] == "committed"
