from pathlib import Path

import pytest

from caravel.cli import make_cli


class _StubPipeline:
    def __init__(self, name: str = "cli_pipeline") -> None:
        self.name = name
        self.stages: list[object] = []


def test_make_cli_executes_without_run_root_and_uses_runner_default() -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(pipeline: object, **kwargs: object) -> Path:
        calls.append({"pipeline": pipeline, **kwargs})
        return Path(".")

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)

    exit_code = cli([])

    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0]["run_root"] is None


def test_make_cli_forwards_run_root_to_runner(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)
    run_root = tmp_path / "custom_root"

    exit_code = cli(["--run-root", str(run_root)])

    assert exit_code == 0
    assert calls[0]["run_root"] == run_root


def test_make_cli_preserves_url_run_root_as_string(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)
    remote_root = "memory://caravel/runs/demo"

    exit_code = cli(["--run-root", remote_root])

    assert exit_code == 0
    assert calls[0]["run_root"] == remote_root


def test_make_cli_forwards_stage_and_step_selectors(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)

    exit_code = cli(["--run-root", str(tmp_path), "--stage", "silver", "--step", "normalize"])

    assert exit_code == 0
    assert calls[0]["only_stage"] == "silver"
    assert calls[0]["only_step"] == "normalize"


def test_make_cli_parses_numeric_stage_step_as_int(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)

    exit_code = cli(["--run-root", str(tmp_path), "--stage", "2", "--step", "03"])

    assert exit_code == 0
    assert calls[0]["only_stage"] == 2
    assert calls[0]["only_step"] == 3


def test_make_cli_forwards_custom_params_map(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)

    exit_code = cli(
        ["--run-root", str(tmp_path), "--param", "refresh=hard", "--param", "lang=en"]
    )

    assert exit_code == 0
    assert calls[0]["params"] == {"refresh": "hard", "lang": "en"}


def test_make_cli_forwards_keep_source_tag_flag(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)

    exit_code = cli(["--run-root", str(tmp_path), "--keep-source-tag"])

    assert exit_code == 0
    assert calls[0]["keep_source_tag"] is True


def test_make_cli_mermaid_mode_emits_graph_without_running_pipeline(tmp_path: Path) -> None:
    run_calls: list[dict[str, object]] = []
    mermaid_calls: list[object] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        run_calls.append(kwargs)
        return tmp_path

    def _fake_mermaid(pipeline: object) -> str:
        mermaid_calls.append(pipeline)
        return "flowchart TD\n  A-->B\n"

    cli = make_cli(_StubPipeline(), run_fn=_fake_run, mermaid_fn=_fake_mermaid)
    out_file = tmp_path / "graph.mmd"

    exit_code = cli(["--mermaid", str(out_file)])

    assert exit_code == 0
    assert len(run_calls) == 0
    assert len(mermaid_calls) == 1
    assert out_file.read_text(encoding="utf-8").startswith("flowchart TD")


def test_make_cli_rejects_mermaid_with_execution_flags() -> None:
    cli = make_cli(_StubPipeline(), run_fn=lambda _pipeline, **_kwargs: Path("."))

    with pytest.raises(SystemExit) as exc:
        cli(["--mermaid", "out.mmd", "--stage", "1"])

    assert exc.value.code == 2


def test_make_cli_mermaid_uses_default_renderer_when_available(tmp_path: Path) -> None:
    run_calls: list[dict[str, object]] = []

    def _fake_run(_pipeline: object, **kwargs: object) -> Path:
        run_calls.append(kwargs)
        return tmp_path

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)
    out_file = tmp_path / "default_graph.mmd"

    exit_code = cli(["--mermaid", str(out_file)])

    assert exit_code == 0
    assert len(run_calls) == 0
    assert out_file.exists()
    assert out_file.read_text(encoding="utf-8").startswith("flowchart TD")


def test_make_cli_selector_validation_error_surfaces_user_message(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_run(_pipeline: object, **_kwargs: object) -> Path:
        raise ValueError("Invalid stage name selector: 'missing'.")

    cli = make_cli(_StubPipeline(), run_fn=_fake_run)

    with pytest.raises(SystemExit) as exc:
        cli(["--run-root", "data/run", "--stage", "missing"])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "Invalid stage name selector" in err


def test_make_cli_help_contains_expected_options(capsys: pytest.CaptureFixture[str]) -> None:
    cli = make_cli(_StubPipeline(), run_fn=lambda _pipeline, **_kwargs: Path("."))

    with pytest.raises(SystemExit) as exc:
        cli(["--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "--run-root" in out
    assert "--stage" in out
    assert "--step" in out
    assert "--param" in out
    assert "--hard-refresh" not in out
    assert "--keep-source-tag" in out
    assert "--mermaid" in out
