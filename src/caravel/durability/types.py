"""Error and version contracts for the v1 durable execution store.

The schema and its state machine live in the C-8 task record's
``references/v1-sql-contract.md``. Errors here carry identifiers and messages,
never credentials, storage options, payloads, or parameter values.
"""

from __future__ import annotations

DURABLE_SCHEMA_VERSION = "v1.0.0"

MAX_IDENTIFIER_LENGTH = 512


class DurabilityError(Exception):
    """Base class for every durable-store failure."""


class UnsupportedStoreVersionError(DurabilityError):
    """Raised when a database states a format or version this build does not read."""


class InvalidTransitionError(DurabilityError):
    """Raised when durable state is asked for a transition it does not define."""


class StoreUnavailableError(DurabilityError):
    """Raised when the recovery database cannot be reached."""


class OutputIntegrityError(DurabilityError):
    """Raised when generated output does not match the payload a Step produced."""


def validate_identifier(value: str, *, what: str) -> str:
    """Return ``value`` when it is a usable durable identifier."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{what} must be a non-empty string.")
    if len(value) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"{what} must be at most {MAX_IDENTIFIER_LENGTH} characters.")
    return value


__all__ = [
    "DURABLE_SCHEMA_VERSION",
    "MAX_IDENTIFIER_LENGTH",
    "DurabilityError",
    "InvalidTransitionError",
    "OutputIntegrityError",
    "StoreUnavailableError",
    "UnsupportedStoreVersionError",
    "validate_identifier",
]
