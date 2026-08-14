import json
import sys
from pathlib import Path
from typing import Any, cast

import pytest

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from examples.saga_rollback.pipeline import run_saga_pipeline
from examples.saga_rollback.__main__ import main


def _read(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def test_successful_saga_commits_business_actions(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.json"
    order = _read(repo_root / "examples" / "saga_rollback" / "data" / "order.json")

    run_saga_pipeline(
        run_root=tmp_path / "runs",
        state_path=state_path,
        journal_path=journal_path,
    )

    assert _read(state_path) == {
        "account_balance": 75,
        "available_inventory": 10 - int(order["quantity"]),
        "shipments": ["shipment-order-001"],
    }
    assert _read(journal_path)["status"] == "committed"


def test_failed_saga_compensates_actions_in_reverse_order(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.json"

    with pytest.raises(RuntimeError, match="Simulated failure"):
        run_saga_pipeline(
            run_root=tmp_path / "runs",
            state_path=state_path,
            journal_path=journal_path,
            fail=True,
        )

    assert _read(state_path) == {
        "account_balance": 100,
        "available_inventory": 10,
        "shipments": [],
    }
    journal = _read(journal_path)
    assert journal["status"] == "compensated"
    assert [entry["action"] for entry in journal["compensations"]] == [
        "release_inventory",
        "refund_account",
        "cancel_shipment",
    ]


def test_cli_succeed_flag_writes_intended_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    assert main(["--succeed"]) == 0

    root = tmp_path / "data" / "output" / "saga_rollback_example"
    assert _read(root / "compensation_journal.json")["status"] == "committed"
    result = _read(
        root
        / "pipeline"
        / "saga_rollback_example"
        / "_001_fulfillment"
        / "_004_finalize_order"
        / "_004_finalize_order.json"
    )
    assert result["status"] == "completed"
    assert result["shipment_id"] == "shipment-order-001"
