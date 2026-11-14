from pathlib import Path

import pytest

from caravel.branch import Branch
from caravel.datasets import JSONDataset
from caravel.pipeline import Pipeline, Stage, Step


class _StubLoader:
    name = "stub_loader"

    def load(self) -> dict[str, dict[str, str]]:
        return {"p1": {"id": "1"}}


class _BadLoader:
    name = "bad_loader"


def _plain_step(inputs, *, context: object) -> dict[str, str]:
    [partition] = inputs.partitions()
    payload: dict[str, str] = partition.load()
    return payload


def test_step_defaults_name_from_function_name() -> None:
    s = Step(fn=_plain_step, output=JSONDataset(name="custom_name"))

    assert s.name == "_plain_step"


def test_step_rejects_non_callable_fn() -> None:
    with pytest.raises(TypeError, match="fn"):
        Step(fn="not_callable", output=JSONDataset(name="x"))  # type: ignore[arg-type]


def test_step_keeps_the_processor_callable_unwrapped() -> None:
    dataset = JSONDataset(name="declared_output")

    def fn(inputs, *, context: object) -> dict[str, str]:
        [partition] = inputs.partitions()
        payload: dict[str, str] = partition.load()
        return payload

    declared = Step(fn, output=dataset)

    assert declared.fn is fn
    stage = Stage(name="bronze", entries=[declared])
    assert stage.entries[0] is declared
    assert declared.output is dataset


def test_step_rejects_non_dataset_output() -> None:
    with pytest.raises(TypeError, match="Dataset"):
        Step(_plain_step, output="not-a-dataset")  # type: ignore[arg-type]


def test_step_rejects_non_bool_persist() -> None:
    with pytest.raises(TypeError, match="persist"):
        Step(_plain_step, output=JSONDataset(name="x"), persist="yes")  # type: ignore[arg-type]


def test_stage_normalizes_bare_callable_to_default_json_dataset_step() -> None:
    stage = Stage(name="bronze", entries=[_plain_step])

    assert len(stage.entries) == 1
    entry = stage.entries[0]
    assert isinstance(entry, Step)
    assert entry.name == "_plain_step"
    assert isinstance(entry.output, JSONDataset)
    assert entry.output.name == "_plain_step"


def test_stage_keeps_the_declared_output_dataset() -> None:
    dataset = JSONDataset(name="declared_dataset")

    stage = Stage(name="bronze", entries=[Step(_plain_step, output=dataset)])

    assert isinstance(stage.entries[0], Step)
    assert stage.entries[0].output is dataset


def test_step_defaults_dataset_name_to_the_step_name_when_omitted() -> None:
    dataset = JSONDataset(indent=2)

    stage = Stage(name="bronze", entries=[Step(_plain_step, output=dataset)])

    entry = stage.entries[0]
    assert isinstance(entry, Step)
    assert isinstance(entry.output, JSONDataset)
    assert entry.output.name == "_plain_step"
    assert entry.output.indent == 2


def test_step_preserves_explicit_custom_dataset_name_over_function_name() -> None:
    explicit = JSONDataset(name="custom_output", indent=4)

    stage = Stage(name="bronze", entries=[Step(_plain_step, output=explicit)])

    entry = stage.entries[0]
    assert isinstance(entry, Step)
    assert isinstance(entry.output, JSONDataset)
    assert entry.output.name == "custom_output"
    assert entry.output.indent == 4


def test_stage_preserves_branch_entries_without_wrapping() -> None:
    branch = Branch(name="by_source", by="source", routes={"a": []})
    stage = Stage(name="bronze", entries=[branch])

    assert stage.entries[0] is branch


def test_stage_rejects_unsupported_entry_type() -> None:
    with pytest.raises(TypeError, match="Stage 'bronze'"):
        Stage(name="bronze", entries=[123])  # type: ignore[list-item]


def test_stage_accepts_stage_root_path_or_url() -> None:
    local_stage = Stage(name="bronze", entries=[_plain_step], stage_root=Path("data/bronze"))
    remote_stage = Stage(name="silver", entries=[_plain_step], stage_root="abfs://container/silver")

    assert local_stage.stage_root == Path("data/bronze")
    assert remote_stage.stage_root == "abfs://container/silver"


def test_stage_rejects_invalid_or_empty_stage_root() -> None:
    with pytest.raises(TypeError, match="stage_root"):
        Stage(name="bronze", entries=[_plain_step], stage_root=123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="cannot be empty"):
        Stage(name="bronze", entries=[_plain_step], stage_root="")


def test_stage_requires_clean_dirs_to_be_bool() -> None:
    with pytest.raises(TypeError, match="clean_dirs"):
        Stage(name="bronze", entries=[_plain_step], clean_dirs="yes")  # type: ignore[arg-type]


def test_pipeline_validates_its_source_and_preserves_stage_order() -> None:
    """A source is optional, but a non-source is rejected."""
    stages = [Stage(name="a", entries=[]), Stage(name="b", entries=[])]
    pipeline = Pipeline(name="p", stages=stages)
    assert [stage.name for stage in pipeline.stages] == ["a", "b"]
    assert pipeline.source is None

    with pytest.raises(TypeError, match="source"):
        Pipeline(name="p", stages=[], source=object())


def test_pipeline_rejects_duplicate_or_empty_stage_names() -> None:
    with pytest.raises(ValueError, match="empty stage name"):
        Pipeline(
            name="demo",
            stages=[Stage(name="", entries=[_plain_step])],
        )

    with pytest.raises(ValueError, match="duplicate stage name"):
        Pipeline(
            name="demo",
            stages=[
                Stage(name="bronze", entries=[_plain_step]),
                Stage(name="bronze", entries=[_plain_step]),
            ],
        )
