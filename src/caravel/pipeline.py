from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .branch import Branch
from .datasets import JSONDataset
from .types import Dataset, Loader, StepFn

_STEP_OUTPUT_ATTR = "__step_output_dataset__"


def _callable_name(fn: Callable[..., Any]) -> str:
    """Return a stable callable name for default step naming."""
    return getattr(fn, "__name__", fn.__class__.__name__)


def _get_decorated_output(fn: Callable[..., Any]) -> Dataset | None:
    output = getattr(fn, _STEP_OUTPUT_ATTR, None)
    if output is None:
        return None

    if not isinstance(output, Dataset):
        raise TypeError(
            f"Callable '{_callable_name(fn)}' has invalid decorated output "
            f"type {type(output).__name__}; expected Dataset."
        )

    return output


def _with_default_dataset_name(output: Dataset, *, step_name: str) -> Dataset:
    """Return dataset with `name` defaulted to `step_name` when omitted."""
    dataset_name = getattr(output, "name", "")

    if dataset_name == "":
        defaulted = copy.deepcopy(output)
        setattr(defaulted, "name", step_name)
        return defaulted

    if not isinstance(dataset_name, str):
        raise TypeError(f"Dataset name must be str, got {type(dataset_name).__name__}.")

    return output


def step(*, output: Dataset) -> Callable[[StepFn], StepFn]:
    """Attach a Dataset to a step function without wrapping callable identity."""
    if not isinstance(output, Dataset):
        raise TypeError(f"step(output=...) expected Dataset, got {type(output).__name__}.")

    def decorator(fn: StepFn) -> StepFn:
        if not callable(fn):
            raise TypeError(f"step decorator expected callable, got {type(fn).__name__}.")
        attached_output = _with_default_dataset_name(output, step_name=_callable_name(fn))
        setattr(fn, _STEP_OUTPUT_ATTR, attached_output)
        return fn

    return decorator


@dataclass
class Step:
    """Normalized executable step declaration."""

    fn: StepFn
    output: Dataset
    name: str | None = None

    def __post_init__(self) -> None:
        if not callable(self.fn):
            raise TypeError(f"Step.fn must be callable, got {type(self.fn).__name__}.")

        if not isinstance(self.output, Dataset):
            raise TypeError(
                f"Step.output must satisfy Dataset protocol, got {type(self.output).__name__}."
            )

        if self.name is None or self.name == "":
            self.name = _callable_name(self.fn)

        if not isinstance(self.name, str):
            raise TypeError(f"Step.name must be str, got {type(self.name).__name__}.")

        self.output = _with_default_dataset_name(self.output, step_name=self.name)


@dataclass
class Stage:
    """Ordered stage declaration containing normalized step and/or branch entries."""

    name: str
    entries: list[Step | Branch | Callable[..., Any]]
    stage_root: Path | str | None = None
    clean_dirs: bool = False

    def __post_init__(self) -> None:
        if self.stage_root is not None:
            if isinstance(self.stage_root, Path):
                pass
            elif isinstance(self.stage_root, str):
                if self.stage_root.strip() == "":
                    raise ValueError("Stage.stage_root cannot be empty when provided.")
            else:
                raise TypeError(
                    f"Stage.stage_root must be Path | str | None, got {type(self.stage_root).__name__}."
                )

        if not isinstance(self.clean_dirs, bool):
            raise TypeError(f"Stage.clean_dirs must be bool, got {type(self.clean_dirs).__name__}.")

        normalized: list[Step | Branch | Callable[..., Any]] = []

        for entry in self.entries:
            if isinstance(entry, (Step, Branch)):
                normalized.append(entry)
                continue

            if callable(entry):
                decorated_output = _get_decorated_output(entry)
                output = decorated_output or JSONDataset()
                normalized.append(Step(fn=entry, output=output))
                continue

            raise TypeError(
                f"Stage '{self.name}' has unsupported entry type "
                f"{type(entry).__name__}; expected Step, Branch, or callable."
            )

        self.entries = normalized


@dataclass
class Pipeline:
    """Top-level pipeline declaration with one loader and ordered stages."""

    name: str
    loader: Loader
    stages: list[Stage]

    def __post_init__(self) -> None:
        if not isinstance(self.loader, Loader):
            raise TypeError(
                f"Pipeline loader must satisfy Loader protocol, got {type(self.loader).__name__}."
            )

        seen_stage_names: set[str] = set()
        normalized_stages: list[Stage] = []

        for stage in self.stages:
            if not isinstance(stage, Stage):
                raise TypeError(f"Pipeline stage must be Stage, got {type(stage).__name__}.")

            if not stage.name:
                raise ValueError("Pipeline contains empty stage name.")

            if stage.name in seen_stage_names:
                raise ValueError(f"Pipeline contains duplicate stage name '{stage.name}'.")

            seen_stage_names.add(stage.name)
            normalized_stages.append(stage)

        self.stages = normalized_stages


__all__ = ["Step", "Stage", "Pipeline", "step"]
