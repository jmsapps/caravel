"""Shared helpers for the example pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from caravel import CommitScope, DurabilityOptions, Pipeline

SCHEMA_VERSION: Literal["v1.0.0"] = "v1.0.0"


def example_data_dir(module_file: str) -> Path:
    """The ``data`` directory beside an example's module.

    Each example keeps its fixtures, its generated output, and its recovery
    database together, so running one leaves nothing outside its own folder.
    """
    return Path(module_file).resolve().parent / "data"


def example_output(module_file: str) -> Path:
    """Where an example writes generated output by default.

    The recovery database resolves to ``.caravel`` beside this directory, so a
    durable run keeps its state in the same ``data`` folder.
    """
    return example_data_dir(module_file) / "output"


def durable(pipeline: Pipeline, job_id: str, *, commit_scope: CommitScope = "step") -> Pipeline:
    """Attach durability options to an example pipeline.

    Examples build their declaration once and choose durability at run time, so
    the same declaration serves a durable run and an in-memory one.
    """
    pipeline.durability_options = DurabilityOptions(
        schema_version=SCHEMA_VERSION, job_id=job_id, commit_scope=commit_scope
    )
    return pipeline


__all__ = ["SCHEMA_VERSION", "durable", "example_data_dir", "example_output"]
