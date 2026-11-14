from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, TypedDict, cast

from caravel import (
    RunResult,
    StepContext,
    Branch,
    CallableLoader,
    JSONDataset,
    Pipeline,
    Stage,
    Step,
    run,
)
from caravel.plugins import RunFacts, RunOutcome

from ..support import durable


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


class Document(TypedDict):
    """One document as the seed dataset supplies it."""

    document_id: str
    text: str
    version: str


class StagedDocument(Document):
    """A document whose chunks are staged."""

    chunk_count: int


class IndexedDocument(StagedDocument):
    """A staged document written into a fresh index."""

    index_recreated: bool


class PromotedDocument(IndexedDocument):
    """An indexed document whose chunks are promoted."""

    chunks_promoted: bool


class CommittedDocument(PromotedDocument):
    """A promoted document the saga committed."""

    status: str


Documents = dict[str, Document]
StagedDocuments = dict[str, StagedDocument]
IndexedDocuments = dict[str, IndexedDocument]
PromotedDocuments = dict[str, PromotedDocument]
CommittedDocuments = dict[str, CommittedDocument]


def build_rag_ingestion_pipeline(
    saga: RagWorkspaceSaga, input_path: Path | str | None = None
) -> Pipeline:
    seed = Path(input_path) if input_path is not None else _DEFAULT_INPUT

    def stage_chunks(value: Documents, *, context: StepContext) -> StagedDocuments:
        documents: StagedDocuments = {}
        saga.record("delete_staging")
        staging = saga.root / "staging"
        shutil.rmtree(staging, ignore_errors=True)
        chunk_index = 0
        for _key, document in value.items():
            words = str(document["text"]).split()
            chunks = [words[index : index + 4] for index in range(0, len(words), 4)]
            for chunk in chunks:
                chunk_index += 1
                _write(
                    staging / f"chunk-{chunk_index:03d}.json",
                    {"text": " ".join(chunk), "vector": [chunk_index, len(chunk)]},
                )
            documents[_key] = StagedDocument(**document, chunk_count=len(chunks))
        return documents

    def replace_index(value: StagedDocuments, *, context: StepContext) -> IndexedDocuments:
        documents: IndexedDocuments = {}
        for key, payload in value.items():
            if key in documents:
                raise ValueError(f"Duplicate document key: {key}")
            documents[key] = IndexedDocument(**payload, index_recreated=True)
        saga.record("restore_index")
        _replace_dir(saga.root / "index", saga.root / "index.backup")
        _write(
            saga.root / "index" / "documents.json",
            {
                "documents": {
                    key: {"version": payload["version"], "chunks": payload["chunk_count"]}
                    for key, payload in documents.items()
                }
            },
        )
        return documents

    def promote_chunks(value: IndexedDocuments, *, context: StepContext) -> PromotedDocuments:
        documents: PromotedDocuments = {}
        for key, payload in value.items():
            if key in documents:
                raise ValueError(f"Duplicate document key: {key}")
            documents[key] = PromotedDocument(**payload, chunks_promoted=True)
        saga.record("restore_current")
        _replace_dir(saga.root / "current", saga.root / "current.backup")
        _replace_dir(saga.root / "staging", saga.root / "current")
        return documents

    def commit_version(value: PromotedDocuments, *, context: StepContext) -> CommittedDocuments:
        documents: CommittedDocuments = {}
        for key, payload in value.items():
            if key in documents:
                raise ValueError(f"Duplicate document key: {key}")
            documents[key] = CommittedDocument(**payload, status="committed")
        shutil.copy2(saga.root / "version.json", saga.root / "version.backup.json")
        saga.record("restore_version")
        _write(
            saga.root / "version.json",
            {"documents": {key: payload["version"] for key, payload in documents.items()}},
        )
        params = getattr(context, "params", {})
        if isinstance(params, dict) and params.get("fail") == "true":
            raise RuntimeError("Simulated failure after chunk promotion")
        return documents

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
) -> RunResult:
    saga = RagWorkspaceSaga(workspace_root)
    return run(
        durable(build_rag_ingestion_pipeline(saga), "rag-1"),
        output=Path(state_root) / "output",
        params={"fail": "true"} if fail else None,
        plugins=[saga],
    )


__all__ = ["PIPELINE_NAME", "RagWorkspaceSaga", "build_rag_ingestion_pipeline", "run_rag_ingestion"]
