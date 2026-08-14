from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, cast

from caravel import Branch, JSONDataset, Pipeline, Stage, Step, dataset_as_loader, run
from caravel.plugins import RunFacts, RunOutcome

PIPELINE_NAME = "saga_rollback_example"
STAGE_NAME = "fulfillment"

_DEFAULT_INPUT_PATH = Path(__file__).resolve().parent / "data" / "order.json"


def _read_json(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _params(context: object) -> Mapping[str, str]:
    params = getattr(context, "params", {})
    return params if isinstance(params, dict) else {}


@dataclass
class SagaTransaction:
    """A small durable compensation journal used by the example steps."""

    state_path: Path
    journal_path: Path
    plugin_id: str = "saga-transaction"

    def enter(self, run: RunFacts) -> None:
        if not self.state_path.exists():
            _write_json(
                self.state_path,
                {"available_inventory": 10, "account_balance": 100, "shipments": []},
            )
        _write_json(
            self.journal_path,
            {"run_id": run.run_id, "status": "active", "compensations": []},
        )

    def exit(self, run: RunFacts, outcome: RunOutcome) -> None:
        _ = run
        journal = _read_json(self.journal_path)
        if outcome.status == "failed":
            for entry in reversed(journal["compensations"]):
                self._compensate(entry)
            journal["status"] = "compensated"
        else:
            journal["status"] = "committed"
        _write_json(self.journal_path, journal)

    def record(self, action: str, **details: Any) -> None:
        journal = _read_json(self.journal_path)
        journal["compensations"].append({"action": action, **details})
        _write_json(self.journal_path, journal)

    def update_state(self, **changes: Any) -> None:
        state = _read_json(self.state_path)
        state.update(changes)
        _write_json(self.state_path, state)

    def state(self) -> dict[str, Any]:
        return _read_json(self.state_path)

    def _compensate(self, entry: Mapping[str, Any]) -> None:
        state = self.state()
        action = entry["action"]
        if action == "release_inventory":
            state["available_inventory"] += entry["quantity"]
        elif action == "refund_account":
            state["account_balance"] += entry["amount"]
        elif action == "cancel_shipment":
            state["shipments"].remove(entry["shipment_id"])
        else:
            raise ValueError(f"Unknown compensation action: {action}")
        _write_json(self.state_path, state)


def build_saga_pipeline(
    transaction: SagaTransaction, input_path: Path | str | None = None
) -> Pipeline:
    seed_path = Path(input_path) if input_path is not None else _DEFAULT_INPUT_PATH

    def reserve_inventory(order: dict[str, Any], *, context: object) -> dict[str, Any]:
        _ = context
        quantity = int(order["quantity"])
        state = transaction.state()
        if state["available_inventory"] < quantity:
            raise ValueError("Insufficient inventory")
        transaction.update_state(available_inventory=state["available_inventory"] - quantity)
        transaction.record("release_inventory", quantity=quantity)
        return {**order, "inventory_reserved": True}

    def charge_account(order: dict[str, Any], *, context: object) -> dict[str, Any]:
        _ = context
        amount = int(order["amount"])
        state = transaction.state()
        if state["account_balance"] < amount:
            raise ValueError("Insufficient account balance")
        transaction.update_state(account_balance=state["account_balance"] - amount)
        transaction.record("refund_account", amount=amount)
        return {**order, "account_charged": True}

    def create_shipment(order: dict[str, Any], *, context: object) -> dict[str, Any]:
        _ = context
        shipment_id = f"shipment-{order['order_id']}"
        state = transaction.state()
        transaction.update_state(shipments=[*state["shipments"], shipment_id])
        transaction.record("cancel_shipment", shipment_id=shipment_id)
        return {**order, "shipment_id": shipment_id}

    def finalize_order(order: dict[str, Any], *, context: object) -> dict[str, Any]:
        if _params(context).get("fail") == "true":
            raise RuntimeError("Simulated failure after all reversible actions")
        return {**order, "status": "completed"}

    steps: list[Step | Branch | Callable[..., Any]] = [
        Step(reserve_inventory, JSONDataset(), name="reserve_inventory"),
        Step(charge_account, JSONDataset(), name="charge_account"),
        Step(create_shipment, JSONDataset(), name="create_shipment"),
        Step(finalize_order, JSONDataset(), name="finalize_order"),
    ]
    loader = dataset_as_loader(JSONDataset(name="order", path=seed_path))
    return Pipeline(name=PIPELINE_NAME, loader=loader, stages=[Stage(STAGE_NAME, steps)])


def run_saga_pipeline(
    *,
    run_root: Path | str,
    state_path: Path,
    journal_path: Path,
    fail: bool = False,
) -> Path | str:
    transaction = SagaTransaction(state_path=state_path, journal_path=journal_path)
    pipeline = build_saga_pipeline(transaction)
    return run(
        pipeline,
        run_root=run_root,
        params={"fail": "true"} if fail else None,
        plugins=[transaction],
    )


__all__ = ["PIPELINE_NAME", "SagaTransaction", "build_saga_pipeline", "run_saga_pipeline"]
