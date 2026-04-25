import logging
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

src_path = Path(__file__).resolve().parents[3]
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pipeline.types import (  # noqa: E402
    SOURCE_FIELD,
    Dataset,
    KeyCollisionError,
    Loader,
    MissingPriorOutputError,
    MissingSourceTagError,
    StepContext,
)


class _StubDataset:
    name = "stub_dataset"

    def load(self) -> dict[str, str]:
        return {"k": "v"}

    def save(self, payload: object, dest: Path) -> None:
        (dest / "saved.txt").write_text(str(payload), encoding="utf-8")

    def exists(self, dest: Path) -> bool:
        return (dest / "saved.txt").exists()

    def describe(self) -> dict[str, str]:
        return {"name": self.name}


class _StubLoader:
    name = "stub_loader"

    def load(self) -> dict[str, dict[str, str]]:
        return {"partition_1": {"id": "1"}}


def test_dataset_and_loader_are_runtime_checkable_protocols(tmp_path: Path) -> None:
    dataset = _StubDataset()
    loader = _StubLoader()

    assert isinstance(dataset, Dataset)
    assert isinstance(loader, Loader)

    dataset.save({"ok": True}, tmp_path)
    assert dataset.exists(tmp_path) is True


def test_step_context_is_frozen_and_has_expected_fields(tmp_path: Path) -> None:
    context = StepContext(
        run_root=tmp_path,
        pipeline_name="demo_pipeline",
        run_id="20260424T010203Z",
        stage_index=1,
        stage_name="bronze",
        step_index=2,
        step_name="normalize",
        step_dir=tmp_path / "_001_bronze" / "_002_normalize",
        prev_step_dir=tmp_path / "_001_bronze" / "_001_extract",
        logger=logging.getLogger("test-step-context"),
        params={"mode": "test"},
    )

    assert context.pipeline_name == "demo_pipeline"
    assert context.stage_name == "bronze"
    assert context.step_name == "normalize"
    assert context.params == {"mode": "test"}

    with pytest.raises(FrozenInstanceError):
        context.step_name = "mutated"  # type: ignore[misc]


def test_source_field_constant_is_reserved_source_tag() -> None:
    assert SOURCE_FIELD == "__source__"


def test_custom_exceptions_are_raisable_and_inherit_exception() -> None:
    for exc_cls in (KeyCollisionError, MissingSourceTagError, MissingPriorOutputError):
        assert issubclass(exc_cls, Exception)

    with pytest.raises(KeyCollisionError, match="collision"):
        raise KeyCollisionError("collision")

    with pytest.raises(MissingSourceTagError, match="source"):
        raise MissingSourceTagError("source tag missing")

    with pytest.raises(MissingPriorOutputError, match="prior"):
        raise MissingPriorOutputError("prior step output missing")
