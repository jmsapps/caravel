"""Bare-core security contracts.

Framework-generated logs and errors must not contain payload contents,
parameter values, or storage-option values.
"""

import logging
from pathlib import Path

import pytest

from caravel.datasets import JSONDataset, PartitionedJSONDataset
from caravel.pipeline import Pipeline, Stage, step

PARAM_CANARY = "PARAM_SECRET_CANARY"
PAYLOAD_CANARY = "PAYLOAD_SECRET_CANARY"
CREDENTIAL_CANARY = "CREDENTIAL_SECRET_CANARY"


class _StubLoader:
    name = "stub_loader"

    def __init__(self, partitions: dict[str, dict[str, object]]) -> None:
        self._partitions = partitions

    def load(self) -> dict[str, dict[str, object]]:
        return self._partitions


def _canary_pipeline(name: str = "canary_pipeline") -> Pipeline:
    @step(output=PartitionedJSONDataset(name="canary_output"))
    def emit(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {key: {**record, "secret": PAYLOAD_CANARY} for key, record in partitions.items()}

    return Pipeline(
        name=name,
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[emit])],
    )


def test_framework_logs_contain_no_payload_or_param_values(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from caravel.runner import run

    with caplog.at_level(logging.DEBUG):
        run(_canary_pipeline(), run_root=tmp_path, params={"api_key": PARAM_CANARY})

    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert joined  # framework did log
    assert PARAM_CANARY not in joined
    assert PAYLOAD_CANARY not in joined


def test_dataset_describe_never_exposes_storage_option_values() -> None:
    dataset = PartitionedJSONDataset(
        name="remote", storage_options={"credential": CREDENTIAL_CANARY}
    )

    described = repr(dataset.describe())

    assert CREDENTIAL_CANARY not in described
    assert "storage_options_configured" in described


def test_validation_errors_name_types_not_values(tmp_path: Path) -> None:
    dataset = PartitionedJSONDataset(name="typed")

    with pytest.raises(TypeError) as exc:
        dataset.save({PAYLOAD_CANARY: object()}, tmp_path / "out")  # not JSON, key is str

    assert PAYLOAD_CANARY not in str(exc.value) or "Partition key" not in str(exc.value)

    with pytest.raises(TypeError) as key_exc:
        dataset.save({1: {"v": PAYLOAD_CANARY}}, tmp_path / "out")

    assert PAYLOAD_CANARY not in str(key_exc.value)


def test_cli_param_errors_do_not_echo_values() -> None:
    from caravel.cli import _parse_params

    with pytest.raises(ValueError) as exc:
        _parse_params([PARAM_CANARY])

    assert PARAM_CANARY not in str(exc.value)

    with pytest.raises(ValueError) as empty_exc:
        _parse_params([f"={PARAM_CANARY}"])

    assert PARAM_CANARY not in str(empty_exc.value)


def test_step_output_dataset_writing_single_file_keeps_data_only_layout(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=JSONDataset(name="single"))
    def summarize(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        return {"count": len(partitions)}

    pipeline = Pipeline(
        name="data_only_single",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[summarize])],
    )

    run(pipeline, run_root=tmp_path)

    step_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_summarize"
    assert sorted(p.name for p in step_dir.iterdir()) == ["_001_summarize.json"]
