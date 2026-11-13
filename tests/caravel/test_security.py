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
    def emit_partition(partition, *, context: object) -> dict[str, dict[str, object]]:
        record: dict[str, object] = partition.load()
        _ = context
        return {"canary": {**record, "secret": PAYLOAD_CANARY}}

    def emit(inputs, *, context):
        """Declare this step's managed input."""
        del context
        return inputs.each(emit_partition)

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
    from caravel import DurableStorage, InMemorySource
    from caravel.runner import run

    with caplog.at_level(logging.DEBUG):
        run(
            _canary_pipeline(),
            job_id="canary",
            source=InMemorySource({"p1": {"secret": PAYLOAD_CANARY}}),
            storage=DurableStorage.at(tmp_path / "output"),
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

    The legacy stage-directory layout is gone with the legacy runner. What must
    still hold is that Caravel never writes metadata, cursors, or identifiers
    alongside a committed payload: authoritative state lives in the store.
    """
    from caravel import DurableStorage, InMemorySource
    from caravel.runner import run

    storage = DurableStorage.at(tmp_path / "output")
    run(
        _canary_pipeline(),
        job_id="layout",
        source=InMemorySource({"p1": {"value": 1}}),
        storage=storage,
    )

    artifacts = Path(str(storage.output.root))
    written = sorted(path for path in artifacts.rglob("*") if path.is_file())
    assert written, "the run committed no artifacts"
    assert all(path.suffix == ".json" for path in written), (
        f"non-payload files written beside committed output: {written}"
    )
    for path in written:
        assert PARAM_CANARY not in path.read_text(encoding="utf-8")
