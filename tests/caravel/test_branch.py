import pytest

from caravel.branch import Branch
from caravel.types import KeyCollisionError, MissingSourceTagError


def test_branch_by_source_groups_partitions_by_source_tag() -> None:
    branch = Branch(name="normalize_by_source", by="source", routes={"a": [], "b": []})
    partitions = {
        "p1": {"id": "1", "__source__": "a"},
        "p2": {"id": "2", "__source__": "b"},
        "p3": {"id": "3", "__source__": "a"},
    }

    grouped = branch.route_partitions(partitions)

    assert set(grouped.keys()) == {"a", "b"}
    assert set(grouped["a"].keys()) == {"p1", "p3"}
    assert set(grouped["b"].keys()) == {"p2"}


def test_branch_by_callable_groups_partitions_by_predicate() -> None:
    branch = Branch(
        name="by_language",
        by=lambda r: r["language"],
        routes={"en": [], "fr": []},
    )
    partitions = {
        "k1": {"language": "en", "id": "1"},
        "k2": {"language": "fr", "id": "2"},
    }

    grouped = branch.route_partitions(partitions)

    assert set(grouped["en"].keys()) == {"k1"}
    assert set(grouped["fr"].keys()) == {"k2"}


def test_branch_by_source_missing_source_tag_raises_missing_source_tag_error() -> None:
    branch = Branch(name="by_source", by="source", routes={"a": []})
    partitions = {"k1": {"id": "1"}}

    with pytest.raises(MissingSourceTagError, match="by_source"):
        branch.route_partitions(partitions)


def test_branch_callable_non_string_route_key_raises_type_error() -> None:
    branch = Branch(name="bad_callable", by=lambda _r: 123, routes={"a": []})  # type: ignore[arg-type]
    partitions = {"k1": {"id": "1"}}

    with pytest.raises(TypeError, match="bad_callable"):
        branch.route_partitions(partitions)


def test_branch_unmapped_route_key_raises_key_error() -> None:
    branch = Branch(name="unmapped", by=lambda _r: "missing_route", routes={"a": []})
    partitions = {"k1": {"id": "1"}}

    with pytest.raises(KeyError, match="missing_route"):
        branch.route_partitions(partitions)


def test_branch_merge_route_outputs_disjoint_keys_success() -> None:
    branch = Branch(name="merge_ok", by="source", routes={"a": [], "b": []})

    route_outputs = {
        "a": {"p1": {"id": "1"}},
        "b": {"p2": {"id": "2"}},
    }
    merged = branch.merge_route_outputs(route_outputs)

    assert list(merged.keys()) == ["p1", "p2"]


def test_branch_merge_route_outputs_overlap_raises_key_collision_error() -> None:
    branch = Branch(name="merge_collision", by="source", routes={"a": [], "b": []})

    route_outputs = {
        "a": {"same": {"id": "1"}},
        "b": {"same": {"id": "2"}},
    }

    with pytest.raises(KeyCollisionError, match="same") as exc:
        branch.merge_route_outputs(route_outputs)

    message = str(exc.value)
    assert "a" in message
    assert "b" in message


def test_branch_routing_does_not_mutate_input_records() -> None:
    branch = Branch(name="no_mutation", by="source", routes={"a": []})

    record = {"id": "1", "__source__": "a"}
    partitions = {"p1": record}

    grouped = branch.route_partitions(partitions)

    assert record == {"id": "1", "__source__": "a"}
    assert grouped["a"]["p1"] is record


def test_branch_merge_order_is_deterministic_from_routes_insertion_order() -> None:
    branch = Branch(name="deterministic", by="source", routes={"a": [], "b": []})

    # Intentionally reversed dict insertion order in input
    route_outputs = {
        "b": {"p2": {"id": "2"}},
        "a": {"p1": {"id": "1"}},
    }

    merged = branch.merge_route_outputs(route_outputs)

    # Merge order follows Branch.routes insertion order (a, then b)
    assert list(merged.keys()) == ["p1", "p2"]
