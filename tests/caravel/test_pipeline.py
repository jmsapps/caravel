import pytest

from caravel.branch import Branch
from caravel.datasets import JSONDataset
from caravel.pipeline import Pipeline, Stage, Step, step


class _StubLoader:
    name = "stub_loader"

    def load(self) -> dict[str, dict[str, str]]:
        return {"p1": {"id": "1"}}


class _BadLoader:
    name = "bad_loader"


def _plain_step(payload: dict[str, str], *, _context: object) -> dict[str, str]:
    return payload


def test_step_defaults_name_from_function_name() -> None:
    s = Step(fn=_plain_step, output=JSONDataset(name="custom_name"))

    assert s.name == "_plain_step"


def test_step_rejects_non_callable_fn() -> None:
    with pytest.raises(TypeError, match="fn"):
        Step(fn="not_callable", output=JSONDataset(name="x"))  # type: ignore[arg-type]


def test_step_decorator_attaches_dataset_metadata_without_wrapping() -> None:
    dataset = JSONDataset(name="decorated_output")

    def fn(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    decorated = step(output=dataset)(fn)

    assert decorated is fn
    stage = Stage(name="bronze", entries=[decorated])
    assert isinstance(stage.entries[0], Step)
    assert stage.entries[0].output is dataset


def test_step_decorator_rejects_non_dataset_output() -> None:
    with pytest.raises(TypeError, match="Dataset"):
        step(output="not-a-dataset")  # type: ignore[arg-type]


def test_step_decorator_last_applied_output_wins() -> None:
    first = JSONDataset(name="first")
    second = JSONDataset(name="second")

    @step(output=first)
    @step(output=second)
    def fn(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    stage = Stage(name="bronze", entries=[fn])
    assert isinstance(stage.entries[0], Step)
    assert stage.entries[0].output is first


def test_stage_normalizes_bare_callable_to_default_json_dataset_step() -> None:
    stage = Stage(name="bronze", entries=[_plain_step])

    assert len(stage.entries) == 1
    entry = stage.entries[0]
    assert isinstance(entry, Step)
    assert entry.name == "_plain_step"
    assert isinstance(entry.output, JSONDataset)
    assert entry.output.name == "_plain_step"


def test_stage_uses_decorator_dataset_when_present() -> None:
    dataset = JSONDataset(name="decorator_dataset")

    @step(output=dataset)
    def decorated(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    stage = Stage(name="bronze", entries=[decorated])

    assert isinstance(stage.entries[0], Step)
    assert stage.entries[0].output is dataset


def test_stage_defaults_decorator_dataset_name_to_function_name_when_omitted() -> None:
    dataset = JSONDataset(indent=2)

    @step(output=dataset)
    def decorated(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    stage = Stage(name="bronze", entries=[decorated])

    assert isinstance(stage.entries[0], Step)
    assert isinstance(stage.entries[0].output, JSONDataset)
    assert stage.entries[0].output.name == "decorated"
    assert stage.entries[0].output.indent == 2


def test_step_preserves_explicit_custom_dataset_name_over_function_name() -> None:
    explicit = JSONDataset(name="custom_output", indent=4)

    @step(output=explicit)
    def decorated(payload: dict[str, str], *, _context: object) -> dict[str, str]:
        return payload

    stage = Stage(name="bronze", entries=[decorated])

    assert isinstance(stage.entries[0], Step)
    assert isinstance(stage.entries[0].output, JSONDataset)
    assert stage.entries[0].output.name == "custom_output"
    assert stage.entries[0].output.indent == 4


def test_stage_preserves_branch_entries_without_wrapping() -> None:
    branch = Branch(name="by_source", by="source", routes={"a": []})
    stage = Stage(name="bronze", entries=[branch])

    assert stage.entries[0] is branch


def test_stage_rejects_unsupported_entry_type() -> None:
    with pytest.raises(TypeError, match="Stage 'bronze'"):
        Stage(name="bronze", entries=[123])  # type: ignore[list-item]


def test_pipeline_requires_loader_protocol_and_preserves_stage_order() -> None:
    stage_a = Stage(name="bronze", entries=[_plain_step])
    stage_b = Stage(name="silver", entries=[_plain_step])

    pipeline = Pipeline(name="demo", loader=_StubLoader(), stages=[stage_a, stage_b])

    assert [stage.name for stage in pipeline.stages] == ["bronze", "silver"]

    with pytest.raises(TypeError, match="loader"):
        Pipeline(name="bad", loader=_BadLoader(), stages=[stage_a])  # type: ignore[arg-type]


def test_pipeline_rejects_duplicate_or_empty_stage_names() -> None:
    with pytest.raises(ValueError, match="empty stage name"):
        Pipeline(
            name="demo",
            loader=_StubLoader(),
            stages=[Stage(name="", entries=[_plain_step])],
        )

    with pytest.raises(ValueError, match="duplicate stage name"):
        Pipeline(
            name="demo",
            loader=_StubLoader(),
            stages=[
                Stage(name="bronze", entries=[_plain_step]),
                Stage(name="bronze", entries=[_plain_step]),
            ],
        )
