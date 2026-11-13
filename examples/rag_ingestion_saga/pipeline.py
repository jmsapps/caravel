from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, cast

from caravel import (
    Branch,
    CallableLoader,
    DurableStorage,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)
from caravel.plugins import RunFacts, RunOutcome

PIPELINE_NAME = "rag_ingestion_saga_example"
_DEFAULT_INPUT = Path(__file__).resolve().parent / "data" / "document.json"


def _read(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _replace_dir(source: Path, target: Path) -> None:
    if target.exists():
        shutil.rmtree(target)
    if source.exists():
        source.rename(target)


@dataclass
class RagWorkspaceSaga:
    """Compensates local stand-ins for chunk storage and a mutable index."""

    root: Path
    plugin_id: str = "rag-workspace-saga"

    @property
    def journal(self) -> Path:
        return self.root / "saga_journal.json"

    def enter(self, run_facts: RunFacts) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not (self.root / "current").exists():
            _write(self.root / "current" / "chunk-001.json", {"text": "previous chunks"})
            _write(self.root / "version.json", {"version": "v1"})
            _write(self.root / "index" / "documents.json", {"version": "v1", "chunks": 1})
        _write(
            self.journal,
            {"run_id": run_facts.invocation_id, "status": "active", "compensations": []},
        )

    def exit(self, run_facts: RunFacts, outcome: RunOutcome) -> None:
        _ = run_facts
        journal = _read(self.journal)
        if outcome.status == "failed":
            for entry in reversed(journal["compensations"]):
                self._compensate(entry["action"])
            journal["status"] = "compensated"
        else:
            self._discard_backups()
            journal["status"] = "committed"
        _write(self.journal, journal)

    def record(self, action: str) -> None:
        journal = _read(self.journal)
        journal["compensations"].append({"action": action})
        _write(self.journal, journal)

    def _compensate(self, action: str) -> None:
        if action == "restore_version":
            shutil.copy2(self.root / "version.backup.json", self.root / "version.json")
        elif action == "restore_current":
            _replace_dir(self.root / "current.backup", self.root / "current")
        elif action == "restore_index":
            _replace_dir(self.root / "index.backup", self.root / "index")
        elif action == "delete_staging":
            shutil.rmtree(self.root / "staging", ignore_errors=True)
        else:
            raise ValueError(f"Unknown compensation: {action}")

    def _discard_backups(self) -> None:
        shutil.rmtree(self.root / "current.backup", ignore_errors=True)
        shutil.rmtree(self.root / "index.backup", ignore_errors=True)
        (self.root / "version.backup.json").unlink(missing_ok=True)


def build_rag_ingestion_pipeline(
    saga: RagWorkspaceSaga, input_path: Path | str | None = None
) -> Pipeline:
    seed = Path(input_path) if input_path is not None else _DEFAULT_INPUT

    def stage_chunks(document: dict[str, Any], *, context: object) -> dict[str, Any]:
        _ = context
        saga.record("delete_staging")
        staging = saga.root / "staging"
        shutil.rmtree(staging, ignore_errors=True)
        words = str(document["text"]).split()
        chunks = [words[index : index + 4] for index in range(0, len(words), 4)]
        for index, chunk in enumerate(chunks, start=1):
            _write(
                staging / f"chunk-{index:03d}.json",
                {"text": " ".join(chunk), "vector": [index, len(chunk)]},
            )
        return {**document, "chunk_count": len(chunks)}

    def replace_index(payload: dict[str, Any], *, context: object) -> dict[str, Any]:
        _ = context
        saga.record("restore_index")
        _replace_dir(saga.root / "index", saga.root / "index.backup")
        _write(
            saga.root / "index" / "documents.json",
            {"version": payload["version"], "chunks": payload["chunk_count"]},
        )
        return {**payload, "index_recreated": True}

    def promote_chunks(payload: dict[str, Any], *, context: object) -> dict[str, Any]:
        _ = context
        saga.record("restore_current")
        _replace_dir(saga.root / "current", saga.root / "current.backup")
        _replace_dir(saga.root / "staging", saga.root / "current")
        return {**payload, "chunks_promoted": True}

    def commit_version(payload: dict[str, Any], *, context: object) -> dict[str, Any]:
        shutil.copy2(saga.root / "version.json", saga.root / "version.backup.json")
        saga.record("restore_version")
        _write(saga.root / "version.json", {"version": payload["version"]})
        params = getattr(context, "params", {})
        if isinstance(params, dict) and params.get("fail") == "true":
            raise RuntimeError("Simulated failure after chunk promotion")
        return {**payload, "status": "committed"}

    entries: list[Step | Branch | Callable[..., Any]] = [
        Step(stage_chunks, JSONDataset(), name="stage_chunks"),
        Step(replace_index, JSONDataset(), name="replace_index"),
        Step(promote_chunks, JSONDataset(), name="promote_chunks"),
        Step(commit_version, JSONDataset(), name="commit_version"),
    ]

    # One document is one execution partition.
    def load_document() -> dict[str, Any]:
        return {"document": cast(dict[str, Any], JSONDataset(name="document", path=seed).load())}

    return Pipeline(
        name=PIPELINE_NAME,
        stages=[Stage("ingestion", entries)],
        source=CallableLoader("document", load_document),
    )


def run_rag_ingestion(
    *, state_root: Path | str, workspace_root: Path, fail: bool = False
) -> Path | str:
    saga = RagWorkspaceSaga(workspace_root)
    return run(
        build_rag_ingestion_pipeline(saga),
        job_id="rag-1",
        storage=DurableStorage.at(Path(state_root) / "output"),
        params={"fail": "true"} if fail else None,
        plugins=[saga],
    )


__all__ = ["PIPELINE_NAME", "RagWorkspaceSaga", "build_rag_ingestion_pipeline", "run_rag_ingestion"]
