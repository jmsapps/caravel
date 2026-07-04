import json
import logging
from dataclasses import dataclass
from pathlib import Path

import fsspec
import pytest

from caravel import Branch
from caravel.datasets import JSONDataset, PartitionedJSONDataset
from caravel.pipeline import Pipeline, Stage, step
from caravel.types import (
    EmptyOutputError,
    KeyCollisionError,
    MissingPriorOutputError,
    UnsupportedCapabilityError,
)


class _StubLoader:
    name = "stub_loader"

    def __init__(self, partitions: dict[str, dict[str, object]]) -> None:
        self._partitions = partitions

    def load(self) -> dict[str, dict[str, object]]:
        return self._partitions


def _memory_run_root(name: str) -> str:
    return f"memory://caravel/test_runner/{name}"


def _default_stage_base(run_root: Path, pipeline_name: str, index: int, stage_name: str) -> Path:
    return run_root / pipeline_name / f"_{index:03d}_{stage_name}"


def _make_linear_pipeline(call_counter: dict[str, int] | None = None) -> Pipeline:
    @step(output=PartitionedJSONDataset(name="bronze_partitions"))
    def bronze_map(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        if call_counter is not None:
            call_counter["bronze_map"] = call_counter.get("bronze_map", 0) + 1
        return {key: {**record, "mapped": True} for key, record in partitions.items()}

    @step(output=JSONDataset(name="silver_summary"))
    def silver_summary(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        if call_counter is not None:
            call_counter["silver_summary"] = call_counter.get("silver_summary", 0) + 1
        return {
            "count": len(partitions),
            "ids": sorted(partitions.keys()),
        }

    return Pipeline(
        name="demo_runner",
        loader=_StubLoader(
            {
                "a": {"id": "a", "value": 1},
                "b": {"id": "b", "value": 2},
            }
        ),
        stages=[
            Stage(name="bronze", entries=[bronze_map]),
            Stage(name="silver", entries=[silver_summary]),
        ],
    )


def test_branch_route_callable_must_be_decorated() -> None:
    from caravel.runner import _normalize_route_step

    def undecorated(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    with pytest.raises(TypeError, match=r"must be decorated with @step"):
        _normalize_route_step(undecorated)


def test_branch_route_normalization_preserves_dataset_storage_options_and_persist() -> None:
    from caravel.runner import _normalize_route_step

    storage_options = {"account_name": "test-account", "credential": "test-credential"}
    output = PartitionedJSONDataset(
        name="azure_output",
        storage_options=storage_options,
    )

    @step(output=output, persist=False)
    def decorated(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    normalized = _normalize_route_step(decorated)

    assert normalized.output is output
    assert normalized.output.storage_options == storage_options  # type: ignore
    assert normalized.persist is False


def test_run_executes_linear_pipeline_and_writes_canonical_stage_step_layout(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    run_root = run(pipeline, run_root=tmp_path)

    assert run_root == tmp_path

    bronze_step_dir = _default_stage_base(tmp_path, pipeline.name, 1, "bronze") / "_001_bronze_map"
    silver_step_dir = (
        _default_stage_base(tmp_path, pipeline.name, 2, "silver") / "_001_silver_summary"
    )

    assert (bronze_step_dir / "a.json").exists()
    assert (bronze_step_dir / "b.json").exists()
    assert (silver_step_dir / "_001_silver_summary.json").exists()

    silver_payload = json.loads((silver_step_dir / "_001_silver_summary.json").read_text("utf-8"))
    assert silver_payload["count"] == 2


def test_run_defaults_run_root_to_data_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    monkeypatch.chdir(tmp_path)

    resolved = run(pipeline)

    assert resolved == Path("data/output")
    bronze_file = Path("data/output") / pipeline.name / "_001_bronze" / "_001_bronze_map" / "a.json"
    assert bronze_file.exists()


def test_run_respects_explicit_run_root_override(tmp_path: Path) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    explicit_root = tmp_path / "explicit_run_root"

    resolved = run(pipeline, run_root=explicit_root)

    assert resolved == explicit_root
    assert (explicit_root / pipeline.name / "_001_bronze" / "_001_bronze_map" / "a.json").exists()


def test_run_supports_remote_memory_run_root_and_writes_outputs() -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    run_root = _memory_run_root("remote_full")

    resolved = run(pipeline, run_root=run_root)
    assert resolved == run_root
    assert isinstance(resolved, str)

    fs, root = fsspec.core.url_to_fs(run_root)
    assert fs.exists(f"{root}/{pipeline.name}/_001_bronze/_001_bronze_map/a.json")
    assert fs.exists(f"{root}/{pipeline.name}/_001_bronze/_001_bronze_map/b.json")
    assert fs.exists(
        f"{root}/{pipeline.name}/_002_silver/_001_silver_summary/_001_silver_summary.json"
    )


def test_stage_root_override_bypasses_default_stage_folder_and_keeps_step_layout(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    run_root = tmp_path / "fallback_run_root"
    stage_root = tmp_path / "bronze_container" / "run_001"
    pipeline.stages[0].stage_root = stage_root

    run(pipeline, run_root=run_root)

    assert (stage_root / "_001_bronze_map" / "a.json").exists()
    assert (stage_root / "_001_bronze_map" / "b.json").exists()
    assert not (stage_root / "_001_bronze").exists()
    assert (
        run_root
        / pipeline.name
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    ).exists()


def test_clean_dirs_true_clears_existing_stage_contents_before_stage_run(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="cleaned_stage"))
    def pass_through(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    stage_root = tmp_path / "stage_clean_target"
    stale_file = stage_root / "stale.txt"
    stage_root.mkdir(parents=True, exist_ok=True)
    stale_file.write_text("stale", encoding="utf-8")

    pipeline = Pipeline(
        name="clean_dirs_pipeline",
        loader=_StubLoader({"a": {"id": "a", "value": 1}}),
        stages=[
            Stage(name="bronze", entries=[pass_through], stage_root=stage_root, clean_dirs=True)
        ],
    )

    run(pipeline, run_root=tmp_path / "fallback_root")

    assert not stale_file.exists()
    assert stage_root.exists()
    assert (stage_root / "_001_pass_through" / "a.json").exists()


def test_clean_dirs_true_without_stage_root_clears_only_the_stage_directory(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="run_root_cleaned"))
    def pass_through(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    pipeline = Pipeline(
        name="run_root_clean_dirs_pipeline",
        loader=_StubLoader({"a": {"id": "a", "value": 1}}),
        stages=[Stage(name="bronze", entries=[pass_through], clean_dirs=True)],
    )

    run_root = tmp_path / "run_root_clean_target"
    foreign_file = run_root / "stale.txt"
    foreign_dir_file = run_root / "obsolete" / "old.json"
    foreign_dir_file.parent.mkdir(parents=True, exist_ok=True)
    foreign_file.write_text("preserve", encoding="utf-8")
    foreign_dir_file.write_text("preserve", encoding="utf-8")

    stage_dir = run_root / pipeline.name / "_001_bronze"
    stale_step_file = stage_dir / "_009_renamed_step" / "left_over.json"
    stale_step_file.parent.mkdir(parents=True, exist_ok=True)
    stale_step_file.write_text("stale", encoding="utf-8")

    run(pipeline, run_root=run_root)

    assert foreign_file.exists()
    assert foreign_dir_file.exists()
    assert not stale_step_file.exists()
    assert (stage_dir / "_001_pass_through" / "a.json").exists()


def test_clean_dirs_true_fails_fast_for_selective_non_first_step_without_deleting(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="first_output"))
    def first_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    @step(output=JSONDataset(name="second_output"))
    def second_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        return {"count": len(partitions)}

    stage_root = tmp_path / "stage_failfast_target"
    sentinel = stage_root / "sentinel.txt"
    stage_root.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("preserve", encoding="utf-8")

    pipeline = Pipeline(
        name="failfast_pipeline",
        loader=_StubLoader({"a": {"id": "a", "value": 1}}),
        stages=[
            Stage(
                name="bronze",
                entries=[first_step, second_step],
                stage_root=stage_root,
                clean_dirs=True,
            )
        ],
    )

    with pytest.raises(ValueError, match="clean_dirs=True cannot be used"):
        run(
            pipeline,
            run_root=tmp_path / "fallback_root",
            only_stage="bronze",
            only_step="second_step",
        )

    assert sentinel.exists()


def test_clean_dirs_on_later_default_stage_preserves_earlier_stage_outputs(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    pipeline.stages[1].clean_dirs = True

    run(pipeline, run_root=tmp_path)

    bronze_file = tmp_path / pipeline.name / "_001_bronze" / "_001_bronze_map" / "a.json"
    silver_file = (
        tmp_path
        / pipeline.name
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    )

    assert bronze_file.exists()
    assert silver_file.exists()


def test_full_rerun_overwrites_existing_step_output(tmp_path: Path) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    run(pipeline, run_root=tmp_path)
    silver_file = (
        tmp_path
        / pipeline.name
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    )
    silver_file.write_text('{"count": 999}', encoding="utf-8")

    run(pipeline, run_root=tmp_path)

    payload = json.loads(silver_file.read_text("utf-8"))
    assert payload["count"] == 2


def test_only_stage_on_later_stage_fails_closed_without_checkpoint_capability(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path)
    before_bronze = calls.get("bronze_map", 0)
    before_silver = calls.get("silver_summary", 0)

    with pytest.raises(UnsupportedCapabilityError, match="checkpoint"):
        run(pipeline, run_root=tmp_path, only_stage="silver")

    assert calls.get("bronze_map", 0) == before_bronze
    assert calls.get("silver_summary", 0) == before_silver


def test_only_stage_on_first_stage_executes_from_loader_seed(tmp_path: Path) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path, only_stage=1)

    assert calls.get("bronze_map", 0) == 1
    assert calls.get("silver_summary", 0) == 0
    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_bronze_map"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]


def test_only_stage_on_later_stage_fails_closed_on_remote_backend() -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)
    run_root = _memory_run_root("remote_only_stage")

    run(pipeline, run_root=run_root)
    before_silver = calls.get("silver_summary", 0)

    with pytest.raises(UnsupportedCapabilityError, match="checkpoint"):
        run(pipeline, run_root=run_root, only_stage="silver")

    assert calls.get("silver_summary", 0) == before_silver


def test_only_step_on_later_stage_fails_closed_and_names_required_upstream(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    with pytest.raises(UnsupportedCapabilityError) as exc:
        run(
            pipeline,
            run_root=tmp_path,
            only_stage="silver",
            only_step="silver_summary",
        )

    assert "stage='bronze'" in str(exc.value)
    assert "step='bronze_map'" in str(exc.value)
    assert "checkpoint" in str(exc.value)


def test_only_step_first_of_first_stage_executes_from_loader_seed(tmp_path: Path) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path, only_stage="bronze", only_step=1)

    assert calls.get("bronze_map", 0) == 1
    assert calls.get("silver_summary", 0) == 0
    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_bronze_map"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]


def test_only_step_needing_same_stage_prior_fails_closed_before_user_code(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    calls = {"first": 0, "second": 0}

    @step(output=PartitionedJSONDataset(name="bronze_partitions"))
    def first_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        calls["first"] += 1
        return {key: {**record, "first": True} for key, record in partitions.items()}

    @step(output=PartitionedJSONDataset(name="bronze_partitions_2"))
    def second_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        calls["second"] += 1
        return dict(partitions)

    pipeline = Pipeline(
        name="two_step_stage",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[first_step, second_step])],
    )

    with pytest.raises(UnsupportedCapabilityError, match="stage='bronze' step='first_step'"):
        run(
            pipeline,
            run_root=tmp_path,
            only_stage="bronze",
            only_step="second_step",
        )

    assert calls == {"first": 0, "second": 0}
    assert not (tmp_path / pipeline.name).exists()


def test_only_step_on_later_stage_fails_closed_on_remote_backend() -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)
    run_root = _memory_run_root("remote_only_step")

    run(pipeline, run_root=run_root)
    before_silver = calls.get("silver_summary", 0)

    with pytest.raises(UnsupportedCapabilityError, match="checkpoint"):
        run(
            pipeline,
            run_root=run_root,
            only_stage="silver",
            only_step="silver_summary",
        )

    assert calls.get("silver_summary", 0) == before_silver


def test_fail_closed_selective_error_is_meaningful_and_logged(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    with caplog.at_level(logging.ERROR):
        with pytest.raises(MissingPriorOutputError) as exc:
            run(pipeline, run_root=tmp_path, only_stage="silver")

    assert isinstance(exc.value, UnsupportedCapabilityError)
    assert "stage='bronze'" in str(exc.value)
    assert "checkpoint" in str(exc.value)
    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert "SELECTIVE FAILURE" in joined
    assert "checkpoint-capability-required" in joined


def test_fail_closed_selective_error_on_remote_backend() -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    run_root = _memory_run_root("remote_missing_prior")

    with pytest.raises(UnsupportedCapabilityError, match="stage='bronze'"):
        run(pipeline, run_root=run_root, only_stage="silver")


def test_invalid_selective_selector_logs_context_before_raise(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError, match="Invalid stage name selector"):
            run(pipeline, run_root=tmp_path, only_stage="does_not_exist")

    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert "SELECTIVE FAILURE" in joined
    assert "invalid-stage-selector" in joined


def test_only_step_target_with_existing_output_still_fails_closed(tmp_path: Path) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path)
    before = calls.get("silver_summary", 0)

    with pytest.raises(UnsupportedCapabilityError, match="checkpoint"):
        run(
            pipeline,
            run_root=tmp_path,
            only_stage="silver",
            only_step="silver_summary",
        )

    assert calls.get("silver_summary", 0) == before


def test_run_passes_custom_params_to_step_context(tmp_path: Path) -> None:
    from caravel.runner import run
    from caravel.datasets import JSONDataset
    from caravel.pipeline import Pipeline, Stage, step

    @step(output=JSONDataset(name="capture_params"))
    def capture_params(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = partitions
        assert hasattr(context, "params")
        return {"params": dict(context.params)}  # type: ignore[attr-defined]

    pipeline = Pipeline(
        name="params_runner",
        loader=_StubLoader({"k": {"id": "k", "value": 1}}),
        stages=[Stage(name="single", entries=[capture_params])],
    )

    run(
        pipeline,
        run_root=tmp_path,
        params={"refresh": "hard", "lang": "en"},
    )

    out_file = (
        tmp_path
        / pipeline.name
        / "_001_single"
        / "_001_capture_params"
        / "_001_capture_params.json"
    )
    payload = json.loads(out_file.read_text("utf-8"))
    assert payload["params"] == {"refresh": "hard", "lang": "en"}


def test_run_passes_custom_context_from_factory_to_step(tmp_path: Path) -> None:
    from caravel.runner import run
    from caravel.types import StepContext

    @dataclass(frozen=True)
    class JobContext:
        base: StepContext
        tenant: str

    @step(output=JSONDataset(name="custom_context"))
    def capture_context(
        partitions: dict[str, dict[str, object]], *, context: JobContext
    ) -> dict[str, object]:
        _ = partitions
        return {
            "tenant": context.tenant,
            "pipeline": context.base.pipeline_name,
            "stage": context.base.stage_name,
            "step": context.base.step_name,
        }

    pipeline = Pipeline(
        name="custom_context_runner",
        loader=_StubLoader({"k": {"id": "k", "value": 1}}),
        stages=[Stage(name="single", entries=[capture_context])],
    )

    run(
        pipeline,
        run_root=tmp_path,
        context_factory=lambda base: JobContext(base=base, tenant="acme"),
    )

    out_file = (
        tmp_path
        / pipeline.name
        / "_001_single"
        / "_001_capture_context"
        / "_001_capture_context.json"
    )
    payload = json.loads(out_file.read_text("utf-8"))
    assert payload == {
        "tenant": "acme",
        "pipeline": "custom_context_runner",
        "stage": "single",
        "step": "capture_context",
    }


def test_run_accepts_context_factory_returning_step_context(tmp_path: Path) -> None:
    from caravel.runner import run
    from caravel.types import StepContext

    @step(output=JSONDataset(name="native_context"))
    def capture_context(
        partitions: dict[str, dict[str, object]], *, context: StepContext
    ) -> dict[str, object]:
        _ = partitions
        return {"step": context.step_name}

    pipeline = Pipeline(
        name="native_context_runner",
        loader=_StubLoader({"k": {"id": "k", "value": 1}}),
        stages=[Stage(name="single", entries=[capture_context])],
    )

    run(pipeline, run_root=tmp_path, context_factory=lambda base: base)

    out_file = (
        tmp_path
        / pipeline.name
        / "_001_single"
        / "_001_capture_context"
        / "_001_capture_context.json"
    )
    payload = json.loads(out_file.read_text("utf-8"))
    assert payload == {"step": "capture_context"}


def test_run_rejects_invalid_custom_context_factory_result(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=JSONDataset(name="unused"))
    def capture_context(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = (partitions, context)
        return {}

    pipeline = Pipeline(
        name="invalid_custom_context_runner",
        loader=_StubLoader({"k": {"id": "k", "value": 1}}),
        stages=[Stage(name="single", entries=[capture_context])],
    )

    with pytest.raises(TypeError, match="context_factory must return StepContext"):
        run(pipeline, run_root=tmp_path, context_factory=lambda _base: object())


def test_branch_route_steps_receive_custom_context(tmp_path: Path) -> None:
    from caravel.runner import run
    from caravel.types import StepContext

    @dataclass(frozen=True)
    class JobContext:
        base: StepContext
        batch_id: str

    @step(output=PartitionedJSONDataset(name="json_norm"))
    def normalize_json(
        partitions: dict[str, dict[str, object]], *, context: JobContext
    ) -> dict[str, dict[str, object]]:
        return {
            key: {
                **record,
                "batch_id": context.batch_id,
                "step": context.base.step_name,
            }
            for key, record in partitions.items()
        }

    branch = Branch(
        name="normalize_by_source",
        by="source",
        routes={"json": [normalize_json]},
    )
    pipeline = Pipeline(
        name="branch_custom_context",
        loader=_StubLoader({"j1": {"id": "j1", "__source__": "json"}}),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    run(
        pipeline,
        run_root=tmp_path,
        context_factory=lambda base: JobContext(base=base, batch_id="batch-001"),
    )

    out_file = (
        tmp_path
        / pipeline.name
        / "_001_bronze"
        / "_001_normalize_by_source"
        / "json"
        / "normalize_json"
        / "j1.json"
    )
    payload = json.loads(out_file.read_text("utf-8"))
    assert payload["batch_id"] == "batch-001"
    assert payload["step"] == "normalize_json"


def test_branch_entry_executes_routes_and_persists_route_lineage_paths(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="json_norm"))
    def normalize_json(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {
            key: {"id": key, "kind": "json", "value": record["value"]}
            for key, record in partitions.items()
        }

    @step(output=PartitionedJSONDataset(name="html_norm"))
    def normalize_html(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {
            key: {"id": key, "kind": "html", "value": record["value"]}
            for key, record in partitions.items()
        }

    branch = Branch(
        name="normalize_by_source",
        by="source",
        routes={
            "json": [normalize_json],
            "html": [normalize_html],
        },
    )

    pipeline = Pipeline(
        name="branch_runner",
        loader=_StubLoader(
            {
                "j1": {"value": 1, "__source__": "json"},
                "h1": {"value": 2, "__source__": "html"},
            }
        ),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    run(pipeline, run_root=tmp_path)

    json_file = (
        tmp_path
        / pipeline.name
        / "_001_bronze"
        / "_001_normalize_by_source"
        / "json"
        / "normalize_json"
        / "j1.json"
    )
    html_file = (
        tmp_path
        / pipeline.name
        / "_001_bronze"
        / "_001_normalize_by_source"
        / "html"
        / "normalize_html"
        / "h1.json"
    )

    assert json_file.exists()
    assert html_file.exists()


def test_branch_route_overlap_propagates_key_collision_error(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="json_norm"))
    def normalize_json(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = (partitions, context)
        return {"same": {"kind": "json"}}

    @step(output=PartitionedJSONDataset(name="html_norm"))
    def normalize_html(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = (partitions, context)
        return {"same": {"kind": "html"}}

    branch = Branch(
        name="normalize_by_source",
        by="source",
        routes={
            "json": [normalize_json],
            "html": [normalize_html],
        },
    )

    pipeline = Pipeline(
        name="branch_collision",
        loader=_StubLoader(
            {
                "j1": {"value": 1, "__source__": "json"},
                "h1": {"value": 2, "__source__": "html"},
            }
        ),
        stages=[Stage(name="bronze", entries=[branch])],
    )

    with pytest.raises(KeyCollisionError, match="same"):
        run(pipeline, run_root=tmp_path)


def test_keep_source_tag_false_strips_source_field_before_save(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="bronze_partitions"))
    def passthrough(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    pipeline = Pipeline(
        name="strip_source",
        loader=_StubLoader({"a": {"id": "a", "__source__": "src_a"}}),
        stages=[Stage(name="bronze", entries=[passthrough])],
    )

    run(pipeline, run_root=tmp_path, keep_source_tag=False)

    stored = json.loads(
        (tmp_path / pipeline.name / "_001_bronze" / "_001_passthrough" / "a.json").read_text(
            "utf-8"
        )
    )
    assert "__source__" not in stored


def test_keep_source_tag_true_preserves_source_field(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="bronze_partitions"))
    def passthrough(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    pipeline = Pipeline(
        name="keep_source",
        loader=_StubLoader({"a": {"id": "a", "__source__": "src_a"}}),
        stages=[Stage(name="bronze", entries=[passthrough])],
    )

    run(pipeline, run_root=tmp_path, keep_source_tag=True)

    stored = json.loads(
        (tmp_path / pipeline.name / "_001_bronze" / "_001_passthrough" / "a.json").read_text(
            "utf-8"
        )
    )
    assert stored["__source__"] == "src_a"


def test_runner_logs_step_start_end_with_dataset_describe_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from caravel import runner

    pipeline = _make_linear_pipeline()

    logger = logging.getLogger("caravel.runner.test")
    logger.setLevel(logging.INFO)

    def _fake_get_logger(
        name: str, debug: bool = True, log_name: str = "app", log_level: int = 20
    ) -> logging.Logger:
        _ = (name, debug, log_name, log_level)
        return logger

    monkeypatch.setattr(runner, "get_logger", _fake_get_logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        runner.run(pipeline, run_root=tmp_path)

    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert "STEP START" in joined
    assert "STEP END" in joined
    assert "checkpoint_written" in joined
    assert "dataset" in joined


def test_full_run_supports_non_persistent_intermediate_steps(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="s1"), persist=False)
    def step_1(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {k: {**v, "a": 1} for k, v in partitions.items()}

    @step(output=PartitionedJSONDataset(name="s2"), persist=False)
    def step_2(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {k: {**v, "b": 2} for k, v in partitions.items()}

    @step(output=JSONDataset(name="final"), persist=True)
    def step_3(partitions: dict[str, dict[str, object]], *, context: object) -> dict[str, object]:
        _ = context
        return {"keys": sorted(partitions.keys()), "sample": partitions["a"]}

    pipeline = Pipeline(
        name="mixed_persist_linear",
        loader=_StubLoader({"a": {"id": "a"}, "b": {"id": "b"}}),
        stages=[Stage(name="bronze", entries=[step_1, step_2, step_3])],
    )
    run(pipeline, run_root=tmp_path)

    base = tmp_path / pipeline.name / "_001_bronze"
    assert not (base / "_001_step_1").exists()
    assert not (base / "_002_step_2").exists()
    out_file = base / "_003_step_3" / "_003_step_3.json"
    assert out_file.exists()
    payload = json.loads(out_file.read_text("utf-8"))
    assert payload["sample"]["a"] == 1
    assert payload["sample"]["b"] == 2


def test_selective_step_fails_when_required_prior_is_non_persistent(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="first"), persist=False)
    def step_1(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    @step(output=JSONDataset(name="second"), persist=True)
    def step_2(partitions: dict[str, dict[str, object]], *, context: object) -> dict[str, object]:
        _ = context
        return {"count": len(partitions)}

    pipeline = Pipeline(
        name="nonpersist_selective_fail",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[step_1, step_2])],
    )

    with pytest.raises(UnsupportedCapabilityError, match="checkpoint"):
        run(pipeline, run_root=tmp_path, only_stage="bronze", only_step="step_2")


def test_empty_partitioned_output_flows_in_memory_through_full_runs(tmp_path: Path) -> None:
    from caravel.runner import run

    calls = {"second": 0}

    @step(output=PartitionedJSONDataset(name="empty", allow_empty=True), persist=True)
    def step_1(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = (partitions, context)
        return {}

    @step(output=JSONDataset(name="summary"), persist=True)
    def step_2(partitions: dict[str, dict[str, object]], *, context: object) -> dict[str, object]:
        _ = context
        calls["second"] += 1
        return {"count": len(partitions)}

    pipeline = Pipeline(
        name="empty_partition_flow",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[step_1, step_2])],
    )

    run(pipeline, run_root=tmp_path)
    run(pipeline, run_root=tmp_path)

    assert calls["second"] == 2
    empty_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_step_1"
    assert empty_dir.exists()
    assert list(empty_dir.iterdir()) == []
    output_file = tmp_path / pipeline.name / "_001_bronze" / "_002_step_2" / "_002_step_2.json"
    assert json.loads(output_file.read_text("utf-8")) == {"count": 0}


def test_persisted_empty_output_fails_at_producing_step_when_disallowed(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="strict_empty"), persist=True)
    def strict_empty(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = (partitions, context)
        return {}

    pipeline = Pipeline(
        name="strict_empty_checkpoint",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[strict_empty])],
    )

    with pytest.raises(EmptyOutputError, match="strict_empty"):
        run(pipeline, run_root=tmp_path)

    output_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_strict_empty"
    assert not output_dir.exists()


def test_branch_route_steps_support_mixed_persistence(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="json_norm"), persist=False)
    def normalize_json(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {k: {**v, "kind": "json"} for k, v in partitions.items()}

    @step(output=PartitionedJSONDataset(name="html_norm"), persist=False)
    def normalize_html(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {k: {**v, "kind": "html"} for k, v in partitions.items()}

    @step(output=PartitionedJSONDataset(name="bronze_converged"), persist=True)
    def converge(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    branch = Branch(
        name="normalize_by_source",
        by="source",
        routes={"json": [normalize_json], "html": [normalize_html]},
    )
    pipeline = Pipeline(
        name="branch_mixed_persist",
        loader=_StubLoader(
            {"j1": {"id": "j1", "__source__": "json"}, "h1": {"id": "h1", "__source__": "html"}}
        ),
        stages=[Stage(name="bronze", entries=[branch, converge])],
    )

    run(pipeline, run_root=tmp_path)

    branch_root = tmp_path / pipeline.name / "_001_bronze" / "_001_normalize_by_source"
    assert not (branch_root / "json" / "normalize_json").exists()
    assert not (branch_root / "html" / "normalize_html").exists()
    assert (tmp_path / pipeline.name / "_001_bronze" / "_002_converge" / "j1.json").exists()
    assert (tmp_path / pipeline.name / "_001_bronze" / "_002_converge" / "h1.json").exists()


class _MutableLoader:
    name = "mutable_loader"

    def __init__(self, partitions: dict[str, dict[str, object]]) -> None:
        self.partitions = partitions

    def load(self) -> dict[str, dict[str, object]]:
        return self.partitions


def _make_mutable_pipeline(loader: _MutableLoader, name: str = "data_safety") -> Pipeline:
    @step(output=PartitionedJSONDataset(name="bronze_partitions"))
    def bronze_map(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {key: {**record, "mapped": True} for key, record in partitions.items()}

    return Pipeline(
        name=name,
        loader=loader,
        stages=[Stage(name="bronze", entries=[bronze_map])],
    )


def test_fewer_key_rerun_replaces_owned_step_directory(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _MutableLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_mutable_pipeline(loader)

    run(pipeline, run_root=tmp_path)
    loader.partitions = {"a": {"id": "a"}}
    run(pipeline, run_root=tmp_path)

    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_bronze_map"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json"]


def test_fewer_key_rerun_replaces_owned_step_directory_on_memory_backend() -> None:
    from caravel.runner import run

    loader = _MutableLoader({"a": {"id": "a"}, "b": {"id": "b"}})
    pipeline = _make_mutable_pipeline(loader, name="data_safety_memory")
    run_root = _memory_run_root("fewer_key_replacement")

    run(pipeline, run_root=run_root)
    loader.partitions = {"a": {"id": "a"}}
    run(pipeline, run_root=run_root)

    fs, root = fsspec.core.url_to_fs(run_root)
    bronze_dir = f"{root}/{pipeline.name}/_001_bronze/_001_bronze_map"
    listed = sorted(str(path).rsplit("/", 1)[-1] for path in fs.ls(bronze_dir, detail=False))
    assert listed == ["a.json"]


def test_invalid_record_type_preserves_prior_output(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _MutableLoader({"a": {"id": "a"}, "b": {"id": "b"}})

    @step(output=PartitionedJSONDataset(name="typed_output"))
    def emit_typed(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        if len(partitions) == 1:
            return {"a": {"id": "a"}, 2: {"id": "bad"}}
        return dict(partitions)

    pipeline = Pipeline(
        name="prior_output_safety",
        loader=loader,
        stages=[Stage(name="bronze", entries=[emit_typed])],
    )

    run(pipeline, run_root=tmp_path)
    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_emit_typed"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]

    loader.partitions = {"a": {"id": "a"}}
    with pytest.raises(TypeError, match="Partition key must be str"):
        run(pipeline, run_root=tmp_path)

    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]


def test_invalid_partition_key_preserves_prior_output(tmp_path: Path) -> None:
    from caravel.runner import run

    loader = _MutableLoader({"a": {"id": "a"}, "b": {"id": "b"}})

    @step(output=PartitionedJSONDataset(name="keyed_output"))
    def emit_keyed(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, object]:
        _ = context
        if len(partitions) == 1:
            return {"a": {"id": "a"}, "../escape": {"id": "bad"}}
        return dict(partitions)

    pipeline = Pipeline(
        name="partition_key_safety",
        loader=loader,
        stages=[Stage(name="bronze", entries=[emit_keyed])],
    )

    run(pipeline, run_root=tmp_path)
    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_emit_keyed"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]

    loader.partitions = {"a": {"id": "a"}}
    with pytest.raises(ValueError):
        run(pipeline, run_root=tmp_path)

    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]


def test_failed_save_leaves_partial_output_that_full_runs_replace(tmp_path: Path) -> None:
    from caravel.runner import run

    class _InterruptibleDataset(PartitionedJSONDataset):
        fail_after_first_write = False

        def save(self, payload: object, dest: Path | str) -> None:
            if not self.fail_after_first_write:
                super().save(payload, dest)
                return
            self.validate_payload(payload)
            first_key = next(iter(payload))  # type: ignore[arg-type]
            super().save({first_key: payload[first_key]}, dest)  # type: ignore[index]
            raise RuntimeError("injected save interruption")

    dataset = _InterruptibleDataset(name="interruptible")

    @step(output=dataset)
    def pass_through(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return dict(partitions)

    pipeline = Pipeline(
        name="interrupted_save",
        loader=_StubLoader({"a": {"id": "a"}, "b": {"id": "b"}}),
        stages=[Stage(name="bronze", entries=[pass_through])],
    )

    dataset.fail_after_first_write = True
    with pytest.raises(RuntimeError, match="injected save interruption"):
        run(pipeline, run_root=tmp_path)

    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_pass_through"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json"]

    dataset.fail_after_first_write = False
    run(pipeline, run_root=tmp_path)
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]


def test_step_output_directories_contain_only_dataset_files(tmp_path: Path) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    run(pipeline, run_root=tmp_path)

    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_bronze_map"
    silver_dir = tmp_path / pipeline.name / "_002_silver" / "_001_silver_summary"
    assert sorted(p.name for p in bronze_dir.iterdir()) == ["a.json", "b.json"]
    assert sorted(p.name for p in silver_dir.iterdir()) == ["_001_silver_summary.json"]


def test_run_id_is_uuid4_hex_and_unique_per_run(tmp_path: Path) -> None:
    from caravel.runner import run

    captured: list[str] = []

    @step(output=PartitionedJSONDataset(name="ctx_probe"))
    def probe(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        captured.append(context.run_id)  # type: ignore[attr-defined]
        return dict(partitions)

    pipeline = Pipeline(
        name="run_id_probe",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[probe])],
    )

    run(pipeline, run_root=tmp_path)
    run(pipeline, run_root=tmp_path)

    assert len(captured) == 2
    first, second = captured
    assert first != second
    for value in captured:
        assert len(value) == 32
        assert value != tmp_path.name
        int(value, 16)


def test_step_after_branch_receives_merged_output(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="pre"), persist=False)
    def pre_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {key: {**record, "pre": True} for key, record in partitions.items()}

    @step(output=PartitionedJSONDataset(name="left"), persist=False)
    def tag_left(partitions: object, *, context: object) -> object:
        _ = context
        return {key: {**record, "route": "left"} for key, record in partitions.items()}  # type: ignore[union-attr]

    @step(output=PartitionedJSONDataset(name="collect"))
    def collect(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return dict(partitions)

    branch = Branch(name="split", by="source", routes={"left": [tag_left]})
    pipeline = Pipeline(
        name="post_branch_flow",
        loader=_StubLoader({"a": {"id": "a", "__source__": "left"}}),
        stages=[Stage(name="bronze", entries=[pre_step, branch, collect])],
    )

    run(pipeline, run_root=tmp_path, keep_source_tag=True)

    collect_dir = tmp_path / pipeline.name / "_001_bronze" / "_003_collect"
    payload = json.loads((collect_dir / "a.json").read_text("utf-8"))
    assert payload["route"] == "left"
    assert payload["pre"] is True


def test_full_run_crosses_stage_boundary_in_memory_with_non_persisted_terminal(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="transient"), persist=False)
    def transient_terminal(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {key: {**record, "transient": True} for key, record in partitions.items()}

    @step(output=JSONDataset(name="final"))
    def final(partitions: dict[str, dict[str, object]], *, context: object) -> dict[str, object]:
        _ = context
        return {
            "count": len(partitions),
            "transient": all(bool(record.get("transient")) for record in partitions.values()),
        }

    pipeline = Pipeline(
        name="memory_stage_crossing",
        loader=_StubLoader({"a": {"id": "a"}, "b": {"id": "b"}}),
        stages=[
            Stage(name="bronze", entries=[transient_terminal]),
            Stage(name="silver", entries=[final]),
        ],
    )

    run(pipeline, run_root=tmp_path)

    final_file = tmp_path / pipeline.name / "_002_silver" / "_001_final" / "_001_final.json"
    assert json.loads(final_file.read_text("utf-8")) == {"count": 2, "transient": True}
    assert not (tmp_path / pipeline.name / "_001_bronze" / "_001_transient_terminal").exists()


def test_non_dict_payload_cannot_cross_stage_boundary(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=JSONDataset(name="summary"))
    def summarize(partitions: dict[str, dict[str, object]], *, context: object) -> object:
        _ = context
        return ["not", "partitions"]

    @step(output=JSONDataset(name="downstream"))
    def downstream(partitions: object, *, context: object) -> object:
        _ = context
        return partitions

    pipeline = Pipeline(
        name="bad_stage_crossing",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[
            Stage(name="bronze", entries=[summarize]),
            Stage(name="silver", entries=[downstream]),
        ],
    )

    with pytest.raises(TypeError, match="must be dict partitions"):
        run(pipeline, run_root=tmp_path)
