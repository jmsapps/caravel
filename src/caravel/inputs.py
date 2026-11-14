"""The calling convention every processor follows.

A processor is ordinary processing code called once per step, as
``fn(value, context=...)``: one positional argument carrying the complete value
the previous step produced, and the keyword-only context. Nothing here runs
during planning.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable

_SENTINEL = object()


def validate_processor_signature(fn: Callable[..., Any], *, what: str) -> None:
    """Accept a processor that can be called as ``fn(value, context=...)``.

    The check binds a signature; it never invokes ``fn``, so validation is safe
    at declaration time. ``functools.partial`` and callable objects are
    supported, because :func:`inspect.signature` already resolves both. A
    callable whose signature cannot be introspected is accepted: deferring the
    error to the first call is better than refusing a working step. Annotations
    decide nothing here -- a signature decides call compatibility only.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return
    try:
        signature.bind(_SENTINEL, context=_SENTINEL)
    except TypeError as error:
        raise TypeError(
            f"{what} must be callable as fn(value, context=...): one positional "
            f"input argument and a keyword-only context argument. {error}"
        ) from error


__all__ = ["validate_processor_signature"]
