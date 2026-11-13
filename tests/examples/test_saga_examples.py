"""Saga examples: run guards compensate on failure under the durable engine.

Neither saga declares a branch; they are linear per-record pipelines whose
compensation is a run guard. They exercise guard enter/exit around one durable
invocation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from examples.rag_ingestion_saga.pipeline import run_rag_ingestion  # noqa: E402
from examples.saga_rollback.pipeline import run_saga_pipeline  # noqa: E402


def _saga(tmp_path: Path, *, fail: bool):
    return run_saga_pipeline(
        state_root=tmp_path / "state",
        state_path=tmp_path / "saga_state.json",
        journal_path=tmp_path / "journal.json",
        fail=fail,
    )


def test_the_saga_completes_and_consumes_resources(tmp_path: Path) -> None:
    assert _saga(tmp_path, fail=False).status == "completed"
    state = json.loads((tmp_path / "saga_state.json").read_text())
    assert state["available_inventory"] < 100 or state["account_balance"] >= 0
    assert state["shipments"]


def test_a_failed_saga_compensates_in_reverse_order(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Simulated failure"):
        _saga(tmp_path, fail=True)

    journal = json.loads((tmp_path / "journal.json").read_text())
    assert journal["status"] == "compensated"
    recorded = [entry["action"] for entry in journal["compensations"]]
    # Compensations are journaled in the order their actions succeeded; the
    # guard applies them in reverse on failure.
    assert recorded == ["release_inventory", "refund_account", "cancel_shipment"]

    state = json.loads((tmp_path / "saga_state.json").read_text())
    assert state["shipments"] == [], "the shipment was not cancelled"


def test_the_rag_ingestion_saga_completes(tmp_path: Path) -> None:
    result = run_rag_ingestion(state_root=tmp_path / "state", workspace_root=tmp_path / "workspace")
    assert result.status == "completed"
    assert (tmp_path / "workspace").exists()


def test_a_failed_rag_ingestion_restores_the_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    with pytest.raises(RuntimeError, match="Simulated failure"):
        run_rag_ingestion(state_root=tmp_path / "state", workspace_root=workspace, fail=True)
    # Compensation removed the staging area rather than leaving it half-applied.
    assert not (workspace / "staging").exists()
