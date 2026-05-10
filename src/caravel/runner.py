from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable, Mapping

from .logger import get_logger
from .branch import Branch
from .paths import format_stage_dir, format_step_dir, resolve_run_root
from .pipeline import Step
from .storage import is_dir, is_url_path, join_path, leaf_name, resolve_fs
from .types import (
    SOURCE_FIELD,
    Dataset,
    KeyCollisionError,
    MissingPriorOutputError,
    Partitions,
    StepContext,
)

_DECORATED_OUTPUT_ATTR = "__step_output_dataset__"
RunPath = Path | str


def _step_name(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__name__", fn.__class__.__name__)


def _step_decl_name(step: Step) -> str:
    if step.name is None:
        raise TypeError("Step.name must be resolved before runner execution.")
    return step.name


def _normalize_route_step(step_like: Step | Callable[..., Any]) -> Step:
    if isinstance(step_like, Step):
        return step_like

    if not callable(step_like):
        raise TypeError(f"Route step must be Step or callable, got {type(step_like).__name__}.")

    decorated_output = getattr(step_like, _DECORATED_OUTPUT_ATTR, None)
    if decorated_output is not None and not isinstance(decorated_output, Dataset):
        raise TypeError(
            f"Route callable '{_step_name(step_like)}' has invalid decorated output "
            f"type {type(decorated_output).__name__}; expected Dataset."
        )

    if isinstance(decorated_output, Dataset):
        output = decorated_output
    else:
        from .datasets import JSONDataset

        output = JSONDataset(name=_step_name(step_like))

    return Step(fn=step_like, output=output)


def _is_remote_path(path: RunPath) -> bool:
    return isinstance(path, str) and is_url_path(path)


def _coerce_run_path(path: Path | str) -> RunPath:
    if isinstance(path, Path):
        return path
    if _is_remote_path(path):
        return path
    return Path(path)


def _join_run_path(base: RunPath, *parts: str) -> RunPath:
    if _is_remote_path(base):
        return join_path(base, *parts)
    resolved = base if isinstance(base, Path) else Path(base)
    return resolved.joinpath(*parts)


def _run_path_name(path: RunPath) -> str:
    if _is_remote_path(path):
        name = leaf_name(path)
        return name or "remote_run"
    resolved = path if isinstance(path, Path) else Path(path)
    return resolved.name


def _dataset_at_path(dataset: Dataset, path: RunPath) -> Dataset:
    bound = copy.copy(dataset)
    if hasattr(bound, "path"):
        dataset_name = type(dataset).__name__
        if _is_remote_path(path):
            target_path: Path | str = path
            path_name = _run_path_name(path)
            if dataset_name == "JSONDataset":
                target_path = join_path(path, f"{path_name}.json")
            elif dataset_name == "TextDataset":
                suffix = str(getattr(dataset, "suffix", ".txt"))
                target_path = join_path(path, f"{path_name}{suffix}")
            elif dataset_name == "BytesDataset":
                suffix = str(getattr(dataset, "suffix", ".bin"))
                target_path = join_path(path, f"{path_name}{suffix}")
        else:
            target_path = Path(path)
            if dataset_name == "JSONDataset":
                target_path = target_path / f"{target_path.name}.json"
            elif dataset_name == "TextDataset":
                suffix = str(getattr(dataset, "suffix", ".txt"))
                target_path = target_path / f"{target_path.name}{suffix}"
            elif dataset_name == "BytesDataset":
                suffix = str(getattr(dataset, "suffix", ".bin"))
                target_path = target_path / f"{target_path.name}{suffix}"

        setattr(bound, "path", target_path)
        return bound
    raise TypeError(
        f"Dataset '{type(dataset).__name__}' does not expose assignable 'path' attribute."
    )


def _load_from_step_output(dataset: Dataset, step_dir: RunPath) -> Any:
    reader = _dataset_at_path(dataset, step_dir)
    return reader.load()


def _strip_source_field(payload: Any, keep_source_tag: bool) -> Any:
    if keep_source_tag:
        return payload

    if not isinstance(payload, dict):
        return payload

    if SOURCE_FIELD in payload:
        stripped = dict(payload)
        stripped.pop(SOURCE_FIELD, None)
        return stripped

    stripped_partitions: dict[str, Any] = {}
    changed = False
    for key, value in payload.items():
        if isinstance(value, dict) and SOURCE_FIELD in value:
            copied = dict(value)
            copied.pop(SOURCE_FIELD, None)
            stripped_partitions[key] = copied
            changed = True
        else:
            stripped_partitions[key] = value

    return stripped_partitions if changed else payload


def _resolve_stage_index(stages: list[Any], selector: str | int | None) -> int | None:
    if selector is None:
        return None

    if isinstance(selector, int):
        if selector < 1 or selector > len(stages):
            raise ValueError(f"Invalid stage index selector: {selector}.")
        return selector - 1

    if isinstance(selector, str):
        for idx, stage in enumerate(stages):
            if stage.name == selector:
                return idx
        raise ValueError(f"Invalid stage name selector: '{selector}'.")

    raise ValueError(f"Invalid stage selector type: {type(selector).__name__}.")


def _resolve_step_entry_index(stage: Any, selector: str | int | None) -> int | None:
    if selector is None:
        return None

    step_entries = [
        (idx, entry) for idx, entry in enumerate(stage.entries) if isinstance(entry, Step)
    ]

    if isinstance(selector, int):
        if selector < 1 or selector > len(step_entries):
            raise ValueError(f"Invalid step index selector: {selector}.")
        return step_entries[selector - 1][0]

    if isinstance(selector, str):
        for idx, entry in step_entries:
            if entry.name == selector:
                return idx
        raise ValueError(f"Invalid step name selector: '{selector}'.")

    raise ValueError(f"Invalid step selector type: {type(selector).__name__}.")


def _missing_prior_error(path: RunPath, stage_name: str, step_name: str) -> MissingPriorOutputError:
    return MissingPriorOutputError(
        "Required prior output missing for selective execution: "
        f"stage='{stage_name}' step='{step_name}' path='{path}'."
    )


def _validate_stage_clean_policy(
    *, clean_dirs: bool, only_step_index: int | None, stage_name: str
) -> None:
    if clean_dirs and only_step_index is not None and only_step_index > 1:
        raise ValueError(
            "Stage clean_dirs=True cannot be used with selective non-first-step execution; "
            f"stage='{stage_name}' step_index={only_step_index}. "
            "Run the full stage, disable clean_dirs, or target step 1."
        )


def _clean_stage_base_if_needed(clean_root: RunPath, clean_dirs: bool) -> None:
    if not clean_dirs:
        return
    fs, root = resolve_fs(clean_root)
    if not fs.exists(root):
        fs.makedirs(root, exist_ok=True)
        return

    if not is_dir(fs, root):
        fs.rm(root)
        fs.makedirs(root, exist_ok=True)
        return

    root_prefix = root.rstrip("/")
    for child in fs.ls(root, detail=False):
        child_path = str(child["name"]) if isinstance(child, dict) else str(child)
        if root_prefix and not child_path.startswith(f"{root_prefix}/") and child_path != root:
            child_path = join_path(root, child_path)
        if child_path == root:
            continue
        fs.rm(child_path, recursive=True)

    fs.makedirs(root, exist_ok=True)


def run(
    pipeline: Any,
    run_root: Path | str | None = None,
    *,
    only_stage: str | int | None = None,
    only_step: str | int | None = None,
    params: Mapping[str, str] | None = None,
    keep_source_tag: bool = False,
) -> RunPath:
    """Execute a pipeline declaration and persist outputs per step."""
    if run_root is not None and _is_remote_path(run_root):
        resolved_run_root: RunPath = run_root
    else:
        resolved_run_root = resolve_run_root(run_root)
        resolved_run_root.mkdir(parents=True, exist_ok=True)

    logger = get_logger(f"caravel.runner.{pipeline.name}", log_name=f"{pipeline.name}_runner")
    is_selective = only_stage is not None or only_step is not None
    run_params = dict(params) if params is not None else {}

    def _log_selective_failure(reason: str, **context: object) -> None:
        if not is_selective:
            return
        context_parts = " ".join(f"{k}={v!r}" for k, v in context.items())
        logger.error(
            "SELECTIVE FAILURE pipeline=%s reason=%s %s",
            pipeline.name,
            reason,
            context_parts,
        )

    try:
        selected_stage_idx = _resolve_stage_index(pipeline.stages, only_stage)
    except ValueError as exc:
        _log_selective_failure(
            "invalid-stage-selector",
            only_stage=only_stage,
            only_step=only_step,
            error=str(exc),
        )
        raise

    if only_step is not None and selected_stage_idx is None:
        _log_selective_failure(
            "only-step-without-stage",
            only_stage=only_stage,
            only_step=only_step,
        )
        raise ValueError("only_step requires only_stage to be set.")

    selected_step_entry_idx: int | None = None
    if selected_stage_idx is not None:
        try:
            selected_step_entry_idx = _resolve_step_entry_index(
                pipeline.stages[selected_stage_idx], only_step
            )
        except ValueError as exc:
            _log_selective_failure(
                "invalid-step-selector",
                only_stage=only_stage,
                only_step=only_step,
                error=str(exc),
            )
            raise

    if selected_stage_idx is None:
        stage_indexes = list(range(len(pipeline.stages)))
    else:
        stage_indexes = [selected_stage_idx]

    def _resolve_stage_base(stage_index: int) -> RunPath:
        stage_decl = pipeline.stages[stage_index]
        if stage_decl.stage_root is not None:
            return _coerce_run_path(stage_decl.stage_root)
        return _join_run_path(
            resolved_run_root,
            pipeline.name,
            format_stage_dir(stage_index + 1, stage_decl.name),
        )

    def _resolve_stage_clean_root(stage_index: int) -> RunPath:
        stage_decl = pipeline.stages[stage_index]
        if stage_decl.stage_root is not None:
            return _coerce_run_path(stage_decl.stage_root)
        return resolved_run_root

    def _load_stage_seed(stage_index: int, target_stage_name: str) -> Partitions:
        if stage_index == 0:
            return pipeline.loader.load()

        prev_stage = pipeline.stages[stage_index - 1]
        if not prev_stage.entries:
            _log_selective_failure(
                "missing-prior-stage-entries",
                target_stage=target_stage_name,
                required_stage=prev_stage.name,
            )
            raise MissingPriorOutputError(
                f"Stage '{prev_stage.name}' has no entries to provide prior output."
            )

        prev_last_entry = prev_stage.entries[-1]
        if not isinstance(prev_last_entry, Step):
            _log_selective_failure(
                "prior-stage-terminal-entry-not-step",
                target_stage=target_stage_name,
                required_stage=prev_stage.name,
                required_entry_type=type(prev_last_entry).__name__,
            )
            raise MissingPriorOutputError(
                "Selective execution across a prior Branch stage is not supported without "
                "re-running the prior stage."
            )

        prev_step_name = _step_decl_name(prev_last_entry)
        prev_stage_dir = _resolve_stage_base(stage_index - 1)
        prev_dir = _join_run_path(
            prev_stage_dir,
            format_step_dir(len(prev_stage.entries), prev_step_name),
        )
        if not prev_last_entry.output.exists(prev_dir):
            _log_selective_failure(
                "missing-prior-output",
                required_stage=prev_stage.name,
                required_step=prev_step_name,
                required_path=str(prev_dir),
            )
            raise _missing_prior_error(prev_dir, prev_stage.name, prev_step_name)

        loaded = _load_from_step_output(prev_last_entry.output, prev_dir)
        if not isinstance(loaded, dict):
            _log_selective_failure(
                "invalid-prior-output-shape",
                required_stage=prev_stage.name,
                required_step=prev_step_name,
                loaded_type=type(loaded).__name__,
            )
            raise TypeError(
                f"Prior output for stage '{prev_stage.name}' step '{prev_step_name}' "
                "must load as dict partitions."
            )
        return loaded

    for stage_index in stage_indexes:
        stage = pipeline.stages[stage_index]
        stage_dir = _resolve_stage_base(stage_index)

        if selected_stage_idx == stage_index and selected_step_entry_idx is not None:
            _validate_stage_clean_policy(
                clean_dirs=stage.clean_dirs,
                only_step_index=selected_step_entry_idx + 1,
                stage_name=stage.name,
            )

        partitions: Partitions = _load_stage_seed(stage_index, stage.name)
        clean_root = _resolve_stage_clean_root(stage_index)
        _clean_stage_base_if_needed(clean_root, stage.clean_dirs)
        previous_step_ref: tuple[Dataset, RunPath, str, str] | None = None

        entry_indexes: list[int]
        if selected_stage_idx == stage_index and selected_step_entry_idx is not None:
            entry_indexes = [selected_step_entry_idx]
        else:
            entry_indexes = list(range(len(stage.entries)))

        if (
            selected_stage_idx == stage_index
            and selected_step_entry_idx is not None
            and selected_step_entry_idx > 0
        ):
            prior_entry_index = selected_step_entry_idx - 1
            prior_entry = stage.entries[prior_entry_index]
            if not isinstance(prior_entry, Step):
                _log_selective_failure(
                    "prior-entry-not-step",
                    stage=stage.name,
                    required_entry_index=prior_entry_index + 1,
                    required_entry_type=type(prior_entry).__name__,
                )
                raise MissingPriorOutputError(
                    "Selective step execution requires a prior Step output in the same stage. "
                    f"Found {type(prior_entry).__name__} at index {prior_entry_index + 1}."
                )

            prior_step_name = _step_decl_name(prior_entry)
            prior_step_dir = _join_run_path(
                stage_dir,
                format_step_dir(prior_entry_index + 1, prior_step_name),
            )
            previous_step_ref = (prior_entry.output, prior_step_dir, stage.name, prior_step_name)

        for entry_index in entry_indexes:
            entry = stage.entries[entry_index]

            if isinstance(entry, Branch):
                branch_dir = _join_run_path(
                    stage_dir, format_step_dir(entry_index + 1, entry.name)
                )
                grouped = entry.route_partitions(partitions)
                route_outputs: dict[str, Partitions] = {}

                for route_key in entry.routes:
                    route_steps = entry.routes[route_key]
                    route_partitions = grouped.get(route_key, {})
                    route_prev_ref: tuple[Dataset, RunPath, str] | None = None
                    route_current: Any = route_partitions

                    for route_step_index, route_step_like in enumerate(route_steps, start=1):
                        route_step = _normalize_route_step(route_step_like)
                        route_step_name = _step_decl_name(route_step)
                        route_step_dir = _join_run_path(branch_dir, route_key, route_step_name)

                        if route_step_index > 1 and route_prev_ref is not None:
                            prev_dataset, prev_dir, prev_name = route_prev_ref
                            if not prev_dataset.exists(prev_dir):
                                raise _missing_prior_error(prev_dir, stage.name, prev_name)
                            route_current = _load_from_step_output(prev_dataset, prev_dir)

                        step_ctx = StepContext(
                            run_root=resolved_run_root,
                            pipeline_name=pipeline.name,
                            run_id=_run_path_name(resolved_run_root),
                            stage_index=stage_index + 1,
                            stage_name=stage.name,
                            step_index=route_step_index,
                            step_name=route_step_name,
                            step_dir=route_step_dir,
                            prev_step_dir=route_prev_ref[1] if route_prev_ref else None,
                            logger=logger,
                            params=run_params,
                        )

                        logger.info(
                            "STEP START pipeline=%s stage=%s step=%s dataset=%s",
                            pipeline.name,
                            stage.name,
                            route_step_name,
                            route_step.output.describe(),
                        )
                        produced = route_step.fn(route_current, context=step_ctx)
                        persisted = _strip_source_field(produced, keep_source_tag)
                        route_step.output.save(persisted, route_step_dir)
                        logger.info(
                            "STEP END pipeline=%s stage=%s step=%s dataset=%s",
                            pipeline.name,
                            stage.name,
                            route_step_name,
                            route_step.output.describe(),
                        )

                        route_prev_ref = (route_step.output, route_step_dir, route_step_name)

                    if route_prev_ref is None:
                        route_outputs[route_key] = route_partitions
                    else:
                        last_dataset, last_dir, last_name = route_prev_ref
                        if not last_dataset.exists(last_dir):
                            raise _missing_prior_error(last_dir, stage.name, last_name)
                        loaded = _load_from_step_output(last_dataset, last_dir)
                        if not isinstance(loaded, dict):
                            raise TypeError(
                                f"Branch route '{route_key}' final output must load as dict partitions."
                            )
                        route_outputs[route_key] = loaded

                try:
                    partitions = entry.merge_route_outputs(route_outputs)
                except KeyCollisionError:
                    raise

                previous_step_ref = None
                continue

            if not isinstance(entry, Step):
                raise TypeError(
                    f"Unsupported stage entry type {type(entry).__name__} in stage '{stage.name}'."
                )

            entry_name = _step_decl_name(entry)
            step_dir = _join_run_path(stage_dir, format_step_dir(entry_index + 1, entry_name))

            explicit_target = (
                selected_stage_idx == stage_index and selected_step_entry_idx == entry_index
            )

            if explicit_target:
                logger.info(
                    "Targeted step recompute pipeline=%s stage=%s step=%s path=%s",
                    pipeline.name,
                    stage.name,
                    entry_name,
                    step_dir,
                )

            step_input = partitions
            if previous_step_ref is not None:
                prev_dataset, prev_dir, prev_stage_name, prev_step_name = previous_step_ref
                if not prev_dataset.exists(prev_dir):
                    logger.error(
                        "Missing prior output pipeline=%s stage=%s step=%s path=%s",
                        pipeline.name,
                        prev_stage_name,
                        prev_step_name,
                        prev_dir,
                    )
                    raise _missing_prior_error(prev_dir, prev_stage_name, prev_step_name)
                step_input = _load_from_step_output(prev_dataset, prev_dir)

            step_ctx = StepContext(
                run_root=resolved_run_root,
                pipeline_name=pipeline.name,
                run_id=_run_path_name(resolved_run_root),
                stage_index=stage_index + 1,
                stage_name=stage.name,
                step_index=entry_index + 1,
                step_name=entry_name,
                step_dir=step_dir,
                prev_step_dir=previous_step_ref[1] if previous_step_ref else None,
                logger=logger,
                params=run_params,
            )

            logger.info(
                "STEP START pipeline=%s stage=%s step=%s dataset=%s",
                pipeline.name,
                stage.name,
                entry_name,
                entry.output.describe(),
            )
            produced = entry.fn(step_input, context=step_ctx)
            persisted = _strip_source_field(produced, keep_source_tag)
            entry.output.save(persisted, step_dir)
            logger.info(
                "STEP END pipeline=%s stage=%s step=%s dataset=%s",
                pipeline.name,
                stage.name,
                entry_name,
                entry.output.describe(),
            )

            partitions = persisted if isinstance(persisted, dict) else partitions
            previous_step_ref = (entry.output, step_dir, stage.name, entry_name)

    return resolved_run_root


__all__ = ["run"]
