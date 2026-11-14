"""The production profile for durable execution.

The recovery database owns committed output and run state, so a production
profile is three decisions:

1. where generated output goes,
2. the durability options the job runs under, and
3. which observers record lifecycle evidence.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from caravel import DurabilityOptions, CommitScope
from caravel.plugins.run_evidence import RunEvidencePlugin
from caravel.storage import join_path


def storage_options_for_url(url: str | Path | None) -> Mapping[str, Any] | None:
    """Return fsspec storage options for ``url``, read from the environment.

    Only Azure URLs need options here: the credential set is supplied by the
    qualification environment rather than by the declaration, so the example
    never holds a secret. Any other scheme resolves through the backend's own
    default credential discovery and returns ``None``.
    """
    rendered = str(url or "")
    if not (rendered.startswith("abfs://") or rendered.startswith("az://")):
        return None
    return _azure_storage_options_from_env()


def _azure_storage_options_from_env() -> dict[str, str] | None:
    """Build Azure credentials from the environment, preferring explicit secrets.

    The order is account key, SAS token, then a complete service principal, and
    finally a bare client id for a user-assigned managed identity. Returning
    ``None`` leaves adlfs to its own discovery.
    """
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    options: dict[str, str] = {}
    if account_name:
        options["account_name"] = account_name

    if account_key:
        options["account_key"] = account_key
    elif sas_token:
        options["sas_token"] = sas_token
    elif tenant_id and client_id and client_secret:
        options["tenant_id"] = tenant_id
        options["client_id"] = client_id
        options["client_secret"] = client_secret
    elif client_id:
        # A user-assigned managed identity may require an explicit client id.
        options["client_id"] = client_id

    return options or None


def build_production_output(state_root: str | Path) -> Path:
    """Where a production job writes its generated output.

    The recovery database lives in ``.caravel`` beside this root, on a local
    filesystem path that survives invocations.
    """
    root = str(state_root).strip()
    if not root:
        raise ValueError("state_root must be a non-empty path")
    return Path(root) / "output"


def build_production_durability(
    job_id: str, *, commit_scope: CommitScope = "step"
) -> DurabilityOptions:
    """Durability options for a production job.

    ``commit_scope="step"`` commits each successful Step, so an interrupted run
    resumes with the most committed progress available.
    """
    return DurabilityOptions(schema_version="v1.0.0", job_id=job_id, commit_scope=commit_scope)


def build_production_plugins(
    metadata_root: str | Path,
    *,
    storage_options: Mapping[str, Any] | None = None,
) -> list[object]:
    """Build the explicit observer composition used for qualification.

    The recovery database owns progress and ownership, so evidence is what a
    plugin contributes.
    """
    root = str(metadata_root).strip()
    if not root:
        raise ValueError("metadata_root must be a non-empty path or fsspec URL")

    return [
        RunEvidencePlugin(
            metadata_root=join_path(root, "evidence"),
            storage_options=storage_options,
        )
    ]
