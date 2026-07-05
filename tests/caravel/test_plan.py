import sys
from pathlib import Path

import pytest

from caravel import Branch
from caravel.datasets import JSONDataset, PartitionedJSONDataset
from caravel.pipeline import Pipeline, Stage, Step, step
from caravel.plan import LogicalPlan, SEED_INPUT, compile_pipeline

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


class _StubLoader:
    name = "stub_loader"

    def __init__(self, partitions: dict[str, dict[str, object]] | None = None) -> None:
        self._partitions = partitions or {}

    def load(self) -> dict[str, dict[str, object]]:
        return self._partitions


def _linear_pipeline(name: str = "linear_demo") -> Pipeline:
    @step(output=PartitionedJSONDataset(name="normalized"), persist=False)
    def normalize(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    @step(output=JSONDataset(name="summary"))
    def summarize(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        return {"count": len(partitions)}

    return Pipeline(
        name=name,
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[normalize, summarize])],
    )


def _branching_pipeline() -> Pipeline:
    @step(output=PartitionedJSONDataset(name="parsed"), persist=False)
    def parse(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    @step(output=PartitionedJSONDataset(name="normalized_json"))
    def normalize(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    @step(output=PartitionedJSONDataset(name="decoded"))
    def decode(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(
        name="by_source",
        by="source",
        routes={"json": [parse, normalize], "text": [decode]},
    )
    return Pipeline(
        name="branch_demo",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )


def test_linear_pipeline_compiles_to_expected_nodes() -> None:
    plan = compile_pipeline(_linear_pipeline())

    assert plan.pipeline_name == "linear_demo"
    assert [node.kind for node in plan.nodes] == ["step", "step"]

    first, second = plan.nodes
    assert first.node_id == "stage-001-entry-001"
    assert first.name == "normalize"
    assert first.input_ref == SEED_INPUT
    assert first.persist is False
    assert first.dataset_type == "PartitionedJSONDataset"
    assert first.dataset_name == "normalized"

    assert second.node_id == "stage-001-entry-002"
    assert second.input_ref == "node:stage-001-entry-001"
    assert second.persist is True
    assert second.dataset_type == "JSONDataset"


def test_branching_pipeline_expands_fan_out_route_steps_and_merge() -> None:
    plan = compile_pipeline(_branching_pipeline())

    kinds = [node.kind for node in plan.nodes]
    assert kinds == ["fan-out", "route-step", "route-step", "route-step", "merge"]

    fan_out, parse_node, normalize_node, decode_node, merge = plan.nodes
    assert fan_out.node_id == "stage-001-entry-001-fanout"
    assert fan_out.input_ref == SEED_INPUT

    assert parse_node.node_id == "stage-001-entry-001-route-001-step-001"
    assert parse_node.input_ref == "route:stage-001-entry-001-fanout:json"
    assert parse_node.route_key == "json"

    assert normalize_node.node_id == "stage-001-entry-001-route-001-step-002"
    assert normalize_node.input_ref == "node:stage-001-entry-001-route-001-step-001"

    assert decode_node.node_id == "stage-001-entry-001-route-002-step-001"
    assert decode_node.input_ref == "route:stage-001-entry-001-fanout:text"
    assert decode_node.route_key == "text"

    assert merge.node_id == "stage-001-entry-001-merge"
    assert merge.merge_input_refs == (
        "node:stage-001-entry-001-route-001-step-002",
        "node:stage-001-entry-001-route-002-step-001",
    )


def test_identical_declarations_produce_byte_stable_snapshots() -> None:
    first = compile_pipeline(_branching_pipeline())
    second = compile_pipeline(_branching_pipeline())

    assert first.to_json() == second.to_json()
    assert compile_pipeline(_linear_pipeline()).to_json() == (
        compile_pipeline(_linear_pipeline()).to_json()
    )


def test_node_ids_are_index_based_and_survive_renames() -> None:
    original = compile_pipeline(_linear_pipeline())
    renamed_pipeline = _linear_pipeline()
    step_entry = renamed_pipeline.stages[0].entries[0]
    assert isinstance(step_entry, Step)
    step_entry.name = "renamed_normalize"

    renamed = compile_pipeline(renamed_pipeline)

    assert [node.node_id for node in original.nodes] == [node.node_id for node in renamed.nodes]
    assert renamed.nodes[0].name == "renamed_normalize"


def test_repeated_route_step_names_across_routes_get_distinct_node_ids() -> None:
    @step(output=PartitionedJSONDataset(name="left"))
    def normalize(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    @step(output=PartitionedJSONDataset(name="right"))
    def normalize_right(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(
        name="dup_names",
        by="source",
        routes={
            "a": [Step(fn=normalize, output=PartitionedJSONDataset(), name="normalize")],
            "b": [Step(fn=normalize_right, output=PartitionedJSONDataset(), name="normalize")],
        },
    )
    pipeline = Pipeline(
        name="dup_across_routes",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    plan = compile_pipeline(pipeline)
    route_nodes = [node for node in plan.nodes if node.kind == "route-step"]
    assert len({node.node_id for node in route_nodes}) == 2
    assert {node.name for node in route_nodes} == {"normalize"}


def test_duplicate_route_step_names_within_one_route_are_rejected() -> None:
    @step(output=PartitionedJSONDataset(name="one"))
    def normalize(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    duplicate = [
        Step(fn=normalize, output=PartitionedJSONDataset(), name="normalize"),
        Step(fn=normalize, output=PartitionedJSONDataset(), name="normalize"),
    ]
    branch = Branch(name="dup_route", by="source", routes={"a": duplicate})
    pipeline = Pipeline(
        name="dup_in_route",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    with pytest.raises(ValueError, match="duplicate step name"):
        compile_pipeline(pipeline)


def test_undecorated_route_callable_is_rejected() -> None:
    def bare(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(name="undecorated", by="source", routes={"a": [bare]})
    pipeline = Pipeline(
        name="undecorated_route",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    with pytest.raises(TypeError, match="must be decorated with @step"):
        compile_pipeline(pipeline)


def test_unsupported_branch_by_shape_is_rejected() -> None:
    @step(output=PartitionedJSONDataset(name="ok"))
    def fine(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(name="bad_by", by=42, routes={"a": [fine]})  # type: ignore[arg-type]
    pipeline = Pipeline(
        name="bad_by_pipeline",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    with pytest.raises(TypeError, match="expected by='source' or callable"):
        compile_pipeline(pipeline)


def test_branch_without_routes_is_rejected() -> None:
    branch = Branch(name="empty_routes", by="source", routes={})
    pipeline = Pipeline(
        name="empty_routes_pipeline",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    with pytest.raises(ValueError, match="at least one route"):
        compile_pipeline(pipeline)


def test_empty_route_step_list_models_pass_through_to_merge() -> None:
    @step(output=PartitionedJSONDataset(name="only"))
    def only(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(name="passthrough", by="source", routes={"a": [only], "b": []})
    pipeline = Pipeline(
        name="passthrough_pipeline",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    plan = compile_pipeline(pipeline)
    merge = plan.nodes[-1]
    assert merge.kind == "merge"
    assert merge.merge_input_refs == (
        "node:stage-001-entry-001-route-001-step-001",
        "route:stage-001-entry-001-fanout:b",
    )


@pytest.mark.parametrize(
    "bad_name",
    ["with/slash", "with\\backslash", "..", ""],
)
def test_path_unsafe_stage_names_are_rejected(bad_name: str) -> None:
    @step(output=JSONDataset(name="x"))
    def fine(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    if bad_name == "":
        with pytest.raises(ValueError):
            Pipeline(
                name="unsafe_stage",
                loader=_StubLoader(),
                stages=[Stage(name=bad_name, entries=[fine])],
            )
        return

    pipeline = Pipeline(
        name="unsafe_stage",
        loader=_StubLoader(),
        stages=[Stage(name=bad_name, entries=[fine])],
    )
    with pytest.raises(ValueError, match="path"):
        compile_pipeline(pipeline)


def test_path_unsafe_route_keys_are_rejected() -> None:
    @step(output=PartitionedJSONDataset(name="x"))
    def fine(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    branch = Branch(name="unsafe_route", by="source", routes={"a/b": [fine]})
    pipeline = Pipeline(
        name="unsafe_route_pipeline",
        loader=_StubLoader(),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    with pytest.raises(ValueError, match="path separators"):
        compile_pipeline(pipeline)


def test_empty_stage_compiles_to_no_nodes_and_preserves_flow() -> None:
    @step(output=JSONDataset(name="final"))
    def final(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    pipeline = Pipeline(
        name="empty_stage_pipeline",
        loader=_StubLoader(),
        stages=[Stage(name="empty", entries=[]), Stage(name="silver", entries=[final])],
    )

    plan = compile_pipeline(pipeline)
    assert len(plan.nodes) == 1
    node = plan.nodes[0]
    assert node.stage_index == 2
    assert node.input_ref == SEED_INPUT


def test_every_example_pipeline_compiles_deterministically() -> None:
    from examples.branching.pipeline import build_branching_pipeline
    from examples.custom_context.pipeline import build_custom_context_pipeline
    from examples.fsspec_minimal.pipeline import build_fsspec_pipeline
    from examples.fsspec_partitioned.pipeline import build_fsspec_partitioned_pipeline
    from examples.minimal.pipeline import build_minimal_pipeline
    from examples.multisource.pipeline import build_multisource_pipeline
    from examples.partitioned.pipeline import build_partitioned_pipeline
    from examples.persist_only_final.pipeline import build_persist_only_final_pipeline

    builders = [
        build_branching_pipeline,
        build_custom_context_pipeline,
        build_fsspec_pipeline,
        build_fsspec_partitioned_pipeline,
        build_minimal_pipeline,
        build_multisource_pipeline,
        build_partitioned_pipeline,
        build_persist_only_final_pipeline,
    ]

    for builder in builders:
        first = compile_pipeline(builder())
        second = compile_pipeline(builder())
        assert isinstance(first, LogicalPlan)
        assert first.to_json() == second.to_json()
        assert first.nodes, f"{builder.__name__} compiled to an empty plan"
        assert len({node.node_id for node in first.nodes}) == len(first.nodes)


def test_snapshot_contains_no_runtime_paths() -> None:
    plan = compile_pipeline(_branching_pipeline())
    snapshot_text = plan.to_json()

    assert "run_root" not in snapshot_text
    assert "output_path" not in snapshot_text
    assert "storage_options" not in snapshot_text
