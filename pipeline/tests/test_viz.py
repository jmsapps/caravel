import re
import sys
from pathlib import Path

src_path = Path(__file__).resolve().parents[3]
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pipeline import Branch, JSONDataset, Pipeline, Stage, step, to_mermaid  # noqa: E402


class _StubLoader:
    name = "stub_loader"

    def load(self) -> dict[str, dict[str, str]]:
        return {"a": {"id": "a"}}


def _build_viz_fixture_pipeline() -> Pipeline:
    @step(output=JSONDataset(name="bronze_first"))
    def normalize(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    branch_entry = Branch(
        name="route_by_source",
        by="source",
        routes={
            "json_source": [],
            "html_source": [],
        },
    )

    @step(output=JSONDataset(name="bronze_after_branch"))
    def merge(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    @step(output=JSONDataset(name="silver_normalize"))
    def normalize_silver(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    return Pipeline(
        name="viz_fixture",
        loader=_StubLoader(),
        stages=[
            Stage(name="bronze", entries=[normalize, branch_entry, merge]),
            Stage(name="silver", entries=[normalize_silver]),
        ],
    )


def _extract_declared_node_ids(mermaid: str) -> list[str]:
    return re.findall(r"^\s*([A-Za-z0-9_]+)\[\"", mermaid, flags=re.MULTILINE)


def test_to_mermaid_emits_flowchart_td_header() -> None:
    mermaid = to_mermaid(_build_viz_fixture_pipeline())
    assert mermaid.startswith("flowchart TD\n")


def test_to_mermaid_preserves_stage_and_step_order() -> None:
    mermaid = to_mermaid(_build_viz_fixture_pipeline())

    bronze_idx = mermaid.index("Stage 1: bronze")
    silver_idx = mermaid.index("Stage 2: silver")
    assert bronze_idx < silver_idx

    step1_idx = mermaid.index("Step 1: normalize")
    branch_idx = mermaid.index("Branch 2: route_by_source")
    step3_idx = mermaid.index("Step 3: merge")
    assert step1_idx < branch_idx < step3_idx


def test_to_mermaid_generates_deterministic_node_ids_without_collisions() -> None:
    mermaid = to_mermaid(_build_viz_fixture_pipeline())
    ids = _extract_declared_node_ids(mermaid)

    assert ids
    assert len(ids) == len(set(ids))
    for node_id in ids:
        assert " " not in node_id


def test_to_mermaid_renders_branch_fan_out_with_all_declared_routes() -> None:
    mermaid = to_mermaid(_build_viz_fixture_pipeline())

    assert "Branch 2: route_by_source" in mermaid
    assert "Route: json_source" in mermaid
    assert "Route: html_source" in mermaid

    route_lines = [line for line in mermaid.splitlines() if "Route: " in line]
    assert len(route_lines) == 2


def test_to_mermaid_output_is_snapshot_stable_for_fixture_pipeline() -> None:
    pipeline = _build_viz_fixture_pipeline()

    first = to_mermaid(pipeline)
    second = to_mermaid(pipeline)

    assert first == second
