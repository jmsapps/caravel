"""A durability demo you can watch.

Three steps do visible work: each one sleeps, logs what it is doing, and
returns its complete result. Supplying ``durability_options`` stores every
completed step's result in a recovery database, so reopening the same job id
restores what finished and runs only what is left.

Run it, interrupt it with an injected failure, then run it again with the same
job id. The steps that committed before the failure are restored without
sleeping, and the log shows exactly which ones.

See ``README.md`` beside this file for a guided session.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from caravel import JSONDataset, Pipeline, RetryOptions, Stage, Step, run, CommitScope
from caravel.durability import InMemorySource

from ..support import durable

PIPELINE_NAME = "durable_demo"

DOCUMENTS = {
    f"doc-{index:03d}": {"id": f"doc-{index:03d}", "words": index * 3} for index in range(1, 9)
}

# Each document takes this long to process, so committed work is worth keeping.
WORK_SECONDS = 0.25


def _log(step: str, symbol: str, message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {step:<10} {symbol} {message}", flush=True)


def _fail_at(context: Any) -> str:
    return str(dict(context.params).get("fail_at", ""))


# Only ``publish`` declares retry options, so that is the step a transient
# failure can be recovered in.
RETRYING_STEP = "publish"


def _flaky_until(step: str, context: Any) -> int:
    if step != RETRYING_STEP:
        return 0
    return int(dict(context.params).get("flaky_attempts", "0") or 0)


_attempts: dict[str, int] = {}


def _work(step: str, documents: dict[str, Any], context: Any) -> None:
    """Sleep per document so the cost of recomputing is visible."""
    _log(step, "▶", f"processing {len(documents)} documents")
    for _ in documents:
        time.sleep(WORK_SECONDS)

    flaky_until = _flaky_until(step, context)
    if flaky_until:
        _attempts[step] = _attempts.get(step, 0) + 1
        if _attempts[step] <= flaky_until:
            _log(step, "↻", f"transient failure on attempt {_attempts[step]}; retrying")
            raise ConnectionError(f"{step} hit a transient failure")

    if _fail_at(context) == step:
        _log(step, "✘", "injected failure")
        raise RuntimeError(f"Injected failure in '{step}'")


def enrich(value: Any, *, context: Any) -> Any:
    """Mark every incoming document as enriched."""
    _work("enrich", value, context)
    result = {key: {**document, "enriched": True} for key, document in value.items()}
    _log("enrich", "✔", f"enriched {len(result)} documents")
    return result


def publish(value: Any, *, context: Any) -> Any:
    """Publish every enriched document."""
    _work("publish", value, context)
    result = {
        key: {"id": document["id"], "words": document["words"], "published": True}
        for key, document in value.items()
    }
    _log("publish", "✔", f"published {len(result)} documents")
    return result


def summarize(value: Any, *, context: Any) -> Any:
    """Summarize the complete published set."""
    _work("summarize", value, context)
    summary = {
        "documents": len(value),
        "words": sum(document["words"] for document in value.values()),
    }
    _log("summarize", "✔", f"{summary['documents']} documents, {summary['words']} words")
    return summary


class StepLogger:
    """Logs which steps ran and which were restored from the database.

    A step that completes without starting was restored: its committed snapshot
    was read back instead of its processor being called.
    """

    plugin_id = "step_logger"
    criticality = "best_effort"

    def __init__(self) -> None:
        self.started: set[str] = set()

    def on_event(self, event: Any) -> None:
        if event.node is None:
            return
        if event.kind == "node_started":
            self.started.add(event.node.name)
        elif event.kind == "node_completed" and event.node.name not in self.started:
            _log(event.node.name, "↺", "restored from snapshot (no work repeated)")


def build_pipeline() -> Pipeline:
    """Build the demo declaration.

    ``publish`` carries retry options, so a transient failure inside it is
    retried in memory before its result is ever committed.
    """
    return Pipeline(
        name=PIPELINE_NAME,
        source=InMemorySource(DOCUMENTS),
        stages=[
            Stage(name="prepare", entries=[Step(enrich, output=JSONDataset(name="enrich"))]),
            Stage(
                name="publish",
                entries=[
                    Step(
                        publish,
                        output=JSONDataset(name="publish"),
                        retry=RetryOptions(max_attempts=4, delay_seconds=lambda attempt: 0.2),
                    )
                ],
            ),
            Stage(
                name="summary",
                entries=[Step(summarize, output=JSONDataset(name="summary"))],
            ),
        ],
    )


def run_demo(
    *,
    output: Path | str,
    job_id: str | None = None,
    commit_scope: CommitScope = "step",
    fail_at: str | None = None,
    flaky_attempts: int = 0,
) -> Any:
    """Run the demo once.

    Without ``job_id`` the run keeps no recovery database and every step runs
    again next time. With one, committed steps are restored.
    """
    _attempts.clear()
    pipeline = build_pipeline()
    if job_id:
        durable(pipeline, job_id, commit_scope=commit_scope)

    params: dict[str, str] = {}
    if fail_at:
        params["fail_at"] = fail_at
    if flaky_attempts:
        params["flaky_attempts"] = str(flaky_attempts)

    return run(
        pipeline,
        output=output,
        params=params or None,
        plugins=[StepLogger()],
    )


def run_to_completion(root: Path, *, job_id: str = "snapshot-v1") -> dict[str, Any]:
    """Run the demo, then reopen it to show committed steps being restored."""
    output = Path(root) / "output"

    first = run_demo(output=output, job_id=job_id)
    reopened = run_demo(output=output, job_id=job_id)

    return {
        "summary": JSONDataset(name="summary").load_from(first.outputs["summarize"]),
        "steps_executed": first.steps_executed,
        "steps_restored": reopened.steps_restored,
        "output_root": str(first.output_root),
    }


__all__ = [
    "DOCUMENTS",
    "PIPELINE_NAME",
    "StepLogger",
    "build_pipeline",
    "enrich",
    "publish",
    "run_demo",
    "run_to_completion",
    "summarize",
]
