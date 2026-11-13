from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .branch import Branch
from .datasets import JSONDataset
from .retry import RetryPolicy
from .types import Dataset, Loader, StepFn

_STEP_OUTPUT_ATTR = "__step_output_dataset__"
_STEP_PERSIST_ATTR = "__step_persist__"


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


def _get_decorated_persist(fn: Callable[..., Any]) -> bool | None:
    persist = getattr(fn, _STEP_PERSIST_ATTR, None)
    if persist is None:
        return None
    if not isinstance(persist, bool):
        raise TypeError(
            f"Callable '{_callable_name(fn)}' has invalid decorated persist "
            f"type {type(persist).__name__}; expected bool."
        )
    return persist


def step(*, output: Dataset, persist: bool = True) -> Callable[[StepFn], StepFn]:
    """Attach a Dataset to a step function without wrapping callable identity."""
    if not isinstance(output, Dataset):
        raise TypeError(f"step(output=...) expected Dataset, got {type(output).__name__}.")
    if not isinstance(persist, bool):
        raise TypeError(f"step(..., persist=...) expected bool, got {type(persist).__name__}.")

    def decorator(fn: StepFn) -> StepFn:
        if not callable(fn):
            raise TypeError(f"step decorator expected callable, got {type(fn).__name__}.")
        attached_output = _with_default_dataset_name(output, step_name=_callable_name(fn))
        setattr(fn, _STEP_OUTPUT_ATTR, attached_output)
        setattr(fn, _STEP_PERSIST_ATTR, persist)
        return fn

    return decorator


@dataclass
class Step:
    """Normalized executable step declaration.

    ``output`` defaults to a JSON dataset named for the processor, so
    ``Step(load)`` is a complete declaration.

    ``input_scope``, ``batch_size``, ``revision``, and ``retry`` configure the
    one durable execution model. Their defaults give whole-partition recovery
    with no retries, which is the simplest useful behavior.

    ``retry`` attaches a policy at the step boundary only. Nested boundaries
    acquire no policy automatically; use the ``retry`` utility explicitly inside
    a processor to retry a batch, record, or single operation.
    """

    fn: StepFn
    output: Dataset | None = None
    persist: bool = True
    name: str | None = None
    input_scope: str = "partition"
    batch_size: int | None = None
    revision: str = "1"
    retry: Any = None

    def __post_init__(self) -> None:
        if not callable(self.fn):
            raise TypeError(f"Step.fn must be callable, got {type(self.fn).__name__}.")

        if self.output is None:
            # A simple declaration needs no dataset: the processor's output is
            # persisted as JSON under the step's own name.
            decorated = _get_decorated_output(self.fn)
            self.output = decorated if decorated is not None else JSONDataset()
        if not isinstance(self.output, Dataset):
            raise TypeError(
                f"Step.output must satisfy Dataset protocol, got {type(self.output).__name__}."
            )

        if not isinstance(self.persist, bool):
            raise TypeError(f"Step.persist must be bool, got {type(self.persist).__name__}.")
        if not self.persist:
            raise ValueError(
                f"Step '{self.name or _callable_name(self.fn)}' sets persist=False, but "
                "durable execution must commit every step's output: a later invocation "
                "restores it instead of recomputing. Remove persist=False."
            )

        if self.name is None or self.name == "":
            self.name = _callable_name(self.fn)

        if not isinstance(self.name, str):
            raise TypeError(f"Step.name must be str, got {type(self.name).__name__}.")

        if self.input_scope not in ("partition", "all"):
            raise ValueError(
                f"Step.input_scope must be 'partition' or 'all', got {self.input_scope!r}."
            )

        if self.batch_size is not None:
            if not isinstance(self.batch_size, int) or isinstance(self.batch_size, bool):
                raise TypeError(
                    f"Step.batch_size must be a positive integer or None, got "
                    f"{type(self.batch_size).__name__}."
                )
            if self.batch_size < 1:
                raise ValueError(
                    f"Step.batch_size must be a positive integer, got {self.batch_size}."
                )

        if not isinstance(self.revision, str) or not self.revision:
            raise ValueError("Step.revision must be a non-empty string.")

        if self.retry is not None and not isinstance(self.retry, RetryPolicy):
            raise TypeError(
                f"Step.retry must be a RetryPolicy or None, got {type(self.retry).__name__}."
            )

        self.output = _with_default_dataset_name(self.output, step_name=self.name)

    @property
    def dataset(self) -> Dataset:
        """The resolved output dataset. Always set once the Step is built."""
        if self.output is None:  # pragma: no cover - guaranteed by __post_init__
            raise TypeError(f"Step '{self.name}' has no resolved output dataset.")
        return self.output


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
                decorated_persist = _get_decorated_persist(entry)
                output = decorated_output or JSONDataset()
                persist = True if decorated_persist is None else decorated_persist
                normalized.append(Step(fn=entry, output=output, persist=persist))
                continue

            raise TypeError(
                f"Stage '{self.name}' has unsupported entry type "
                f"{type(entry).__name__}; expected Step, Branch, or callable."
            )

        self.entries = normalized


@dataclass
class Pipeline:
    """Top-level pipeline declaration: a name, ordered stages, and an input source.

    ``source`` is optional on the declaration because execution supplies it.
    Anything satisfying the :class:`~caravel.types.Loader` protocol is accepted
    and adapted, so a loader remains a valid way to describe input.
    """

    name: str
    stages: list[Stage]
    source: Any = None

    def __post_init__(self) -> None:
        if self.source is not None and not (
            isinstance(self.source, Loader) or hasattr(self.source, "discover")
        ):
            raise TypeError(
                "Pipeline source must satisfy the Loader protocol or provide "
                f"discover(), got {type(self.source).__name__}."
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
