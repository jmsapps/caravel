"""A generic durable partition pipeline spanning several bounded invocations.

The job processes documents one partition at a time, commits each partition's
output durably, and summarizes the whole run behind an explicit all-input
barrier. An invocation budget forces the work across several invocations, and a
consumer reads committed batches from its own durable position while production
is still ongoing.

``DurableStorage.at(<root>/output)`` publishes readable results there and
keeps reserved execution storage in ``.caravel`` beside it:

.. code-block:: text

    <root>/
      .caravel/                            reserved; required for recovery
        state.sqlite3
        attempts/...                       immutable, uuid-named
      output/examples/durable-partitions/snapshot-v1/
        001-prepare/001-enrich/doc-001.json
        002-publish/001-publish/doc-001.json
        003-summary/001-summarize/__all__.json
        _job.json                          advisory completion snapshot

Nothing here is domain specific: the "documents" are plain dictionaries.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import caravel
from caravel import JSONDataset, Pipeline, Stage, Step
from caravel.durability import (
    DurableStorage,
    BatchFeed,
    ExecutionBudget,
    InMemorySource,
)
from caravel import RetryOptions

PIPELINE_ID = "examples/durable-partitions"
PUBLISH_NODE = "stage-002-entry-001"

DOCUMENTS = {
    f"doc-{index:03d}": {"id": f"doc-{index:03d}", "words": index * 3} for index in range(1, 9)
}


# Stand-in for real work, so the invocation budget actually forces the job
# across several invocations instead of finishing in one.
WORK_SECONDS = 0.004


def enrich_partition(partition, *, context: Any) -> Any:
    """Partition-scoped work: runs as soon as its own partition is ready."""
    document: Any = partition.load()
    del context
    time.sleep(WORK_SECONDS)
    return {**document, "enriched": True}


def enrich(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(enrich_partition)


def publish_partition(partition, *, context: Any) -> Any:
    """Destination publication is an ordinary partition-scoped step."""
    document: Any = partition.load()
    del context
    time.sleep(WORK_SECONDS)
    return {"id": document["id"], "words": document["words"], "published": True}


def publish(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.each(publish_partition)


def summarize_all(partitions, *, context: Any) -> Any:
    """All-input barrier: waits for every required partition, then runs once.

    It receives a reader over committed references rather than a materialized
    dataset, so finalization stays bounded.
    """
    dataset = partitions
    del context
    total = 0
    count = 0
    for partition in dataset.partitions():
        total += partition.load()["words"]
        count += 1
    return {"documents": count, "words": total}


def summarize(inputs, *, context):
    """Declare this step's managed input."""
    del context
    return inputs.all(summarize_all)


def build_pipeline() -> Pipeline:
    # Retry options attached at the step boundary. Nested boundaries acquire
    # none automatically; the same instance can be reused at any boundary.
    vendor_retry = RetryOptions(max_attempts=4)
    return Pipeline(
        name="durable-documents",
        stages=[
            Stage(name="prepare", entries=[Step(fn=enrich, output=JSONDataset(name="enrich"))]),
            Stage(
                name="publish",
                entries=[Step(fn=publish, output=JSONDataset(name="publish"), retry=vendor_retry)],
            ),
            Stage(
                name="summary",
                entries=[
                    Step(
                        fn=summarize,
                        output=JSONDataset(name="summary"),
                    )
                ],
            ),
        ],
    )


def run_to_completion(root: Path, *, job_id: str = "snapshot-v1") -> dict[str, Any]:
    """Invoke the job repeatedly until it completes, as a scheduler would."""
    storage = DurableStorage.at(Path(root) / "output")
    store = storage.progress
    output = storage.output
    pipeline = build_pipeline()

    consumed: list[str] = []
    invocations = 0
    try:
        feed = BatchFeed(
            store,
            pipeline_id=PIPELINE_ID,
            job_id=job_id,
            node_id=PUBLISH_NODE,
            output=output,
        )
        while True:
            invocations += 1
            result = caravel.run(
                pipeline,
                pipeline_id=PIPELINE_ID,
                job_id=job_id,
                source=InMemorySource(DOCUMENTS),
                storage=storage,
                # A small budget forces the job across several invocations.
                budget=ExecutionBudget(seconds=0.03, max_unit_seconds=0.008),
            )

            # An incremental consumer reads committed batches before the
            # producer finishes, from its own durable position.
            page = feed.read_for("indexer")
            for entry in page.entries:
                consumed.append(entry.partition_id)
            if page.entries:
                feed.acknowledge("indexer", page.next_position)

            if result.status != "yielded":
                break
            if invocations > 200:
                raise RuntimeError("the job failed to converge")

        return {
            "status": result.status,
            "invocations": invocations,
            "consumed_before_completion": consumed,
            "published_root": str(storage.output_root),
        }
    finally:
        store.close()
