"""Bare-core security contracts.

Framework-generated logs and errors must not contain payload contents,
parameter values, or storage-option values.
"""

import logging
from pathlib import Path

import pytest

from caravel.datasets import PartitionedJSONDataset
from caravel.pipeline import Pipeline, Stage, Step

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
    def emit(value, *, context: object) -> dict[str, dict[str, object]]:
        _ = context
        record: dict[str, object] = dict(next(iter(value.values())))
        return {"canary": {**record, "secret": PAYLOAD_CANARY}}

    return Pipeline(
        name=name,
        stages=[
            Stage(
                name="bronze",
                entries=[Step(emit, output=PartitionedJSONDataset(name="canary_output"))],
            )
        ],
    )


def test_framework_logs_contain_no_payload_or_param_values(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from caravel import DurabilityOptions, InMemorySource
    from caravel.runner import run

    pipeline = _canary_pipeline()
    pipeline.durability_options = DurabilityOptions(schema_version="v1.0.0", job_id="canary")
    with caplog.at_level(logging.DEBUG):
        run(
            pipeline,
            source=InMemorySource({"p1": {"secret": PAYLOAD_CANARY}}),
            output=tmp_path / "output",
            params={"api_key": PARAM_CANARY},
        )

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


def test_committed_artifacts_contain_only_payload_files(tmp_path: Path) -> None:
    """Durable artifact locations hold payload data and nothing else.

    Caravel writes payload files and nothing else beside generated output:
    authoritative state lives in the recovery database.
    """
    from caravel import DurabilityOptions, InMemorySource
    from caravel.runner import run

    pipeline = _canary_pipeline()
    pipeline.durability_options = DurabilityOptions(schema_version="v1.0.0", job_id="layout")
    result = run(
        pipeline,
        source=InMemorySource({"p1": {"value": 1}}),
        output=tmp_path / "output",
    )

    artifacts = Path(str(result.output_root))
    written = sorted(path for path in artifacts.rglob("*") if path.is_file())
    assert written, "the run committed no artifacts"
    assert all(path.suffix == ".json" for path in written), (
        f"non-payload files written beside committed output: {written}"
    )
    for path in written:
        assert PARAM_CANARY not in path.read_text(encoding="utf-8")
