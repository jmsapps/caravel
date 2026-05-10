import json
import logging
from pathlib import Path

import fsspec
import pytest

from caravel import Branch
from caravel.datasets import JSONDataset, PartitionedJSONDataset
from caravel.pipeline import Pipeline, Stage, step
from caravel.types import KeyCollisionError, MissingPriorOutputError


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


def test_run_executes_linear_pipeline_and_writes_canonical_stage_step_layout(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    run_root = run(pipeline, run_root=tmp_path)

    assert run_root == tmp_path

    bronze_step_dir = _default_stage_base(tmp_path, pipeline.name, 1, "bronze") / "_001_bronze_map"
    silver_step_dir = _default_stage_base(tmp_path, pipeline.name, 2, "silver") / "_001_silver_summary"

    assert (bronze_step_dir / "a.json").exists()
    assert (bronze_step_dir / "b.json").exists()
    assert (silver_step_dir / "_001_silver_summary.json").exists()

    silver_payload = json.loads((silver_step_dir / "_001_silver_summary.json").read_text("utf-8"))
    assert silver_payload["count"] == 2


def test_run_defaults_run_root_to_data_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    monkeypatch.chdir(tmp_path)

    resolved = run(pipeline)

    assert resolved == Path("data/output")
    bronze_file = (
        Path("data/output")
        / pipeline.name
        / "_001_bronze"
        / "_001_bronze_map"
        / "a.json"
    )
    assert bronze_file.exists()


def test_run_respects_explicit_run_root_override(tmp_path: Path) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    explicit_root = tmp_path / "explicit_run_root"

    resolved = run(pipeline, run_root=explicit_root)

    assert resolved == explicit_root
    assert (
        explicit_root / pipeline.name / "_001_bronze" / "_001_bronze_map" / "a.json"
    ).exists()


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
        stages=[Stage(name="bronze", entries=[pass_through], stage_root=stage_root, clean_dirs=True)],
    )

    run(pipeline, run_root=tmp_path / "fallback_root")

    assert not stale_file.exists()
    assert stage_root.exists()
    assert (stage_root / "_001_pass_through" / "a.json").exists()


def test_clean_dirs_true_without_stage_root_clears_run_root_contents_before_stage_run(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="run_root_cleaned"))
    def pass_through(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return partitions

    run_root = tmp_path / "run_root_clean_target"
    stale_file = run_root / "stale.txt"
    stale_dir_file = run_root / "obsolete" / "old.json"
    stale_dir_file.parent.mkdir(parents=True, exist_ok=True)
    stale_file.write_text("stale", encoding="utf-8")
    stale_dir_file.write_text("stale", encoding="utf-8")

    pipeline = Pipeline(
        name="run_root_clean_dirs_pipeline",
        loader=_StubLoader({"a": {"id": "a", "value": 1}}),
        stages=[Stage(name="bronze", entries=[pass_through], clean_dirs=True)],
    )

    run(pipeline, run_root=run_root)

    assert run_root.exists()
    assert not stale_file.exists()
    assert not stale_dir_file.exists()
    assert (
        run_root
        / pipeline.name
        / "_001_bronze"
        / "_001_pass_through"
        / "a.json"
    ).exists()


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


def test_clean_dirs_on_later_default_stage_loads_seed_before_run_root_cleanup(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    pipeline.stages[1].clean_dirs = True

    run(pipeline, run_root=tmp_path)

    bronze_file = (
        tmp_path
        / pipeline.name
        / "_001_bronze"
        / "_001_bronze_map"
        / "a.json"
    )
    silver_file = (
        tmp_path
        / pipeline.name
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    )

    assert not bronze_file.exists()
    assert silver_file.exists()


def test_default_collision_behavior_overwrites_existing_step_output(tmp_path: Path) -> None:
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

    run(pipeline, run_root=tmp_path, only_stage="silver")

    payload = json.loads(silver_file.read_text("utf-8"))
    assert payload["count"] == 2


def test_only_stage_by_name_executes_target_stage_with_prior_load_from_disk(tmp_path: Path) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path)
    before_bronze = calls.get("bronze_map", 0)
    before_silver = calls.get("silver_summary", 0)

    run(pipeline, run_root=tmp_path, only_stage="silver")

    assert calls.get("bronze_map", 0) == before_bronze
    assert calls.get("silver_summary", 0) == before_silver + 1


def test_only_stage_by_index_executes_target_stage(tmp_path: Path) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path)
    before_bronze = calls.get("bronze_map", 0)
    before_silver = calls.get("silver_summary", 0)

    run(pipeline, run_root=tmp_path, only_stage=2)

    assert calls.get("bronze_map", 0) == before_bronze
    assert calls.get("silver_summary", 0) == before_silver + 1


def test_only_stage_by_name_executes_target_stage_with_remote_prior_load() -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)
    run_root = _memory_run_root("remote_only_stage")

    run(pipeline, run_root=run_root)
    before_bronze = calls.get("bronze_map", 0)
    before_silver = calls.get("silver_summary", 0)

    run(pipeline, run_root=run_root, only_stage="silver")

    assert calls.get("bronze_map", 0) == before_bronze
    assert calls.get("silver_summary", 0) == before_silver + 1


def test_only_step_by_name_executes_target_step_and_requires_prior_output(
    tmp_path: Path,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    with pytest.raises(MissingPriorOutputError, match="_001_bronze"):
        run(
            pipeline,
            run_root=tmp_path,
            only_stage="silver",
            only_step="silver_summary",
        )


def test_only_step_by_index_executes_target_step(tmp_path: Path) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    bronze_dir = tmp_path / pipeline.name / "_001_bronze" / "_001_bronze_map"
    bronze_dataset = PartitionedJSONDataset(name="seed", path=bronze_dir)
    bronze_dataset.save(
        {
            "x": {"id": "x", "mapped": True},
            "y": {"id": "y", "mapped": True},
        },
        bronze_dir,
    )

    run(
        pipeline,
        run_root=tmp_path,
        only_stage="silver",
        only_step=1,
    )

    silver_file = (
        tmp_path
        / pipeline.name
        / "_002_silver"
        / "_001_silver_summary"
        / "_001_silver_summary.json"
    )
    payload = json.loads(silver_file.read_text("utf-8"))
    assert payload["count"] == 2


def test_only_step_by_index_requires_prior_step_output_from_same_stage(tmp_path: Path) -> None:
    from caravel.runner import run

    @step(output=PartitionedJSONDataset(name="bronze_partitions"))
    def first_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {key: {**record, "first": True} for key, record in partitions.items()}

    @step(output=PartitionedJSONDataset(name="bronze_partitions_2"))
    def second_step(
        partitions: dict[str, dict[str, object]], *, context: object
    ) -> dict[str, dict[str, object]]:
        _ = context
        return {
            key: {**record, "second": record.get("first", False)}
            for key, record in partitions.items()
        }

    pipeline = Pipeline(
        name="two_step_stage",
        loader=_StubLoader({"a": {"id": "a"}}),
        stages=[Stage(name="bronze", entries=[first_step, second_step])],
    )

    with pytest.raises(MissingPriorOutputError, match="_001_first_step"):
        run(
            pipeline,
            run_root=tmp_path,
            only_stage="bronze",
            only_step="second_step",
        )


def test_only_step_by_name_executes_target_step_with_remote_prior_output() -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)
    run_root = _memory_run_root("remote_only_step")

    run(pipeline, run_root=run_root)
    before_bronze = calls.get("bronze_map", 0)
    before_silver = calls.get("silver_summary", 0)

    run(
        pipeline,
        run_root=run_root,
        only_stage="silver",
        only_step="silver_summary",
    )

    assert calls.get("bronze_map", 0) == before_bronze
    assert calls.get("silver_summary", 0) == before_silver + 1


def test_missing_prior_output_under_selective_execution_raises_meaningful_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()

    with caplog.at_level(logging.ERROR):
        with pytest.raises(MissingPriorOutputError, match="Required prior output missing") as exc:
            run(pipeline, run_root=tmp_path, only_stage="silver")

    assert "stage='bronze'" in str(exc.value)
    assert "path='" in str(exc.value)
    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert "SELECTIVE FAILURE" in joined
    assert "missing-prior-output" in joined


def test_missing_prior_output_under_remote_selective_execution_raises_meaningful_error() -> None:
    from caravel.runner import run

    pipeline = _make_linear_pipeline()
    run_root = _memory_run_root("remote_missing_prior")

    with pytest.raises(MissingPriorOutputError, match="Required prior output missing") as exc:
        run(pipeline, run_root=run_root, only_stage="silver")

    assert "memory://caravel/test_runner/remote_missing_prior" in str(exc.value)


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


def test_only_step_target_recomputes_when_output_already_exists(tmp_path: Path) -> None:
    from caravel.runner import run

    calls: dict[str, int] = {}
    pipeline = _make_linear_pipeline(call_counter=calls)

    run(pipeline, run_root=tmp_path)
    before = calls.get("silver_summary", 0)

    run(
        pipeline,
        run_root=tmp_path,
        only_stage="silver",
        only_step="silver_summary",
    )

    assert calls.get("silver_summary", 0) == before + 1


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
    assert "dataset" in joined
