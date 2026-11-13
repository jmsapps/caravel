![Caravel Logo](./public/logo.png)

# Caravel: A Lightweight Pipeline Runner

Caravel is a small Python framework for declaring and running deterministic data
pipelines. It supports ordered stages, ordered steps, source-aware loading,
branch routing, dataset-owned persistence, and optional Mermaid graph output.
Applications can extend runner behavior through explicit plugins. Bare Caravel
deliberately never treats existing output files as evidence of completed work.

This project is currently a pipeline runner with branch fan-out/fan-in support,
not a general-purpose DAG scheduler. It does not yet provide arbitrary dependency
edges, topological sorting, cycle detection, distributed execution, or durable
orchestrator state.

## Purpose

Use this framework to declare and run deterministic data pipelines with:

- typed pipeline primitives (`Pipeline`, `Stage`, `Step`, `Branch`)
- dataset-owned I/O (`JSONDataset`, `Partitioned*Dataset`, etc.)
- source-aware loading (`MultiSourceLoader`)
- deterministic on-disk output layout
- selective execution of slices that need no prior output
- explicit observer, run-guard, and checkpoint-policy plugin capabilities
- optional Mermaid graph rendering

## Project Layout

```text
src/caravel/        # installable library package
examples/           # runnable example pipelines
tests/caravel/      # framework tests
tests/examples/     # example pipeline tests
```

## Setup

Install Caravel in editable mode with test dependencies:

```bash
python3 -m pip install -e ".[test]"
```

Install only the dependencies used by the examples:

```bash
python3 -m pip install -e ".[examples]"
```

Install with cloud extras when needed:

```bash
python3 -m pip install -e ".[azure]"
python3 -m pip install -e ".[gcp]"
python3 -m pip install -e ".[s3]"
```

Run the same local checks enforced by CI:

```bash
python3 -m pytest -q
python3 -m ruff check src tests examples
python3 -m ruff format --check src tests examples
python3 -m mypy src/caravel
```

## Core Declaration Pattern

A pipeline is a name and ordered stages. Steps are plain functions: each one
receives **one execution partition's payload** and returns that partition's
output.

```python
from caravel import DurableStorage, InMemorySource, Pipeline, Stage, Step, run

def load(document, *, context):
    return {**document, "loaded": True}

def enrich(document, *, context):
    return {**document, "enriched": True}

def summarize(dataset, *, context):
    # An all-input step receives a reader over committed partitions,
    # never an eagerly materialized dataset.
    return {"documents": sum(1 for _ in dataset.partitions())}

pipeline = Pipeline(
    name="documents",
    stages=[
        Stage(name="prepare", entries=[Step(load), Step(enrich)]),
        Stage(name="summary", entries=[Step(summarize, input_scope="all")]),
    ],
)

result = run(
    pipeline,
    job_id="snapshot-2026-11-12",
    source=InMemorySource({"doc-1": {...}, "doc-2": {...}}),
    storage=DurableStorage.at("state/"),
)
```

`Step(load)` is complete: its output defaults to a JSON dataset named for the
processor. No loader stub and no dataset are required.

### One execution model

There is one engine, reached through `run()`. There is no `durable=True` and no
second entry point: batching, retries, storage, and concurrency all configure
the same model. A step always receives one partition unless it declares
`input_scope="all"`.

### Storage is an explicit decision

`run()` requires `storage` and never defaults it, because durability is the one
thing a caller must not get by accident.

| Choice | Meaning |
| --- | --- |
| `DurableStorage.at(path)` | Authoritative state under `path`, surviving invocations. Requires an explicit `job_id`. `result.restart_safe` is `True`. |
| `DurableStorage.at(path, artifacts_root="s3://bucket/run")` | Same, with committed artifacts in object storage. The authoritative store stays local. |
| `DurableStorage.ephemeral()` | Explicit opt-out. Same state machine and same commit rules — only the persistence guarantee is lost. `job_id` is optional and `result.restart_safe` is `False`. |

### Reading committed output

```python
storage = DurableStorage.at("state/")
results = storage.results(pipeline, job_id="snapshot-2026-11-12")
results.of("enrich")        # {partition_id: payload}
results.feed("enrich")      # a bounded, ordered consumer feed
```

## Dataset Quick Reference

| Dataset                   | Typical payload                | Writes                                 |
| ------------------------- | ------------------------------ | -------------------------------------- |
| `JSONDataset`             | Any JSON-serializable object   | Single JSON file in step folder        |
| `PartitionedJSONDataset`  | `dict[str, JSON-serializable]` | One JSON file per partition key        |
| `TextDataset`             | `str`                          | Single text file (configurable suffix) |
| `PartitionedTextDataset`  | `dict[str, str]`               | One text file per partition key        |
| `BytesDataset`            | `bytes`                        | Single binary file                     |
| `PartitionedBytesDataset` | `dict[str, bytes]`             | One binary file per partition key      |

Partitioned datasets reject empty mappings by default. Set `allow_empty=True`
when an empty result is valid:

```python
output = PartitionedJSONDataset(name="optional_records", allow_empty=True)
```

Dataset output directories are data-only; Caravel writes no sentinel or
control files next to your data. An allowed empty output is simply an empty
step directory, and on object stores without durable empty directories it
leaves no loadable artifact — durable empty-output evidence is a
checkpoint-plugin concern. With the default `allow_empty=False`, saving `{}`
raises `EmptyOutputError` at the producing step before any prior output is
touched.

Partition keys may use path-style nesting like `en/record_001`; partitioned
datasets resolve those keys into nested output directories.

Dataset `path` values can be local paths or `fsspec` URLs such as:

- `./examples/fsspec/data/input_partitions.json`
- `abfs://caravel/input/input_partitions.json`
- `gs://my-bucket/prefix/input_partitions.json`
- `s3://my-bucket/prefix/input_partitions.json`

## Plugins

Two capabilities remain, because only these extend behaviour the engine does
not already own.

- **Observers** receive lifecycle events after committed transitions
  (`run_started`, `node_started`, `node_completed`, `node_failed`,
  `run_completed`, `run_failed`). Each declares `criticality` as `"required"`
  or `"best_effort"`; a required failure fails the run, a best-effort one does
  not. An observer can never undo what already committed.
- **Run guards** wrap one invocation in an external precondition. They enter in
  plugin order and exit in reverse, and teardown is attempted for every guard
  whose startup completed.

```python
run(pipeline, job_id="j1", source=..., storage=..., plugins=[MyObserver()])
```

`CheckpointPlugin`, `OwnershipPlugin`, and `LeasePlugin` were **retired**: the
authoritative progress store owns committed-output evidence, cross-run
inventory, and worker leases, so a plugin can no longer contradict it.

## Multi-Source Input And Branches

`MultiSourceLoader` tags each record with `__source__` and is accepted directly
as a `source`. The tag is preserved through commits: durable output is an
immutable artifact, so nothing rewrites a payload before committing it.
(The retired runner stripped the tag by default — see the breaking-change note
below.)

### Branches

A `Branch` routes each partition to one of several step chains, then merges.

Routing happens **at the branch**, not at discovery, so a predicate may read
values earlier steps produced. The decision is committed per partition together
with that route's work in a single transaction, which means the predicate is
consulted exactly once per partition even across restarts — a flapping or
externally-dependent predicate cannot re-route a record after a crash.

The merge is partition-scoped: each partition merges as soon as its own route
seals. That is the right fit for per-partition routing, but it does **not**
subsume a whole-dataset merge — for that, add an `input_scope="all"` step after
the merge.

A route step transforms one partition's payload and must preserve that
partition's identity. Repartitioning belongs in an explicit `input_scope="all"`
step outside the branch.

## Breaking Changes From The Single-Pass Runner

| Removed | Replacement |
| --- | --- |
| `run(pipeline, run_root)` | `run(pipeline, job_id=..., source=..., storage=...)` |
| `only_stage` / `only_step` | Re-invoking a job already skips committed work. Forcing one step to recompute is **designed but not implemented**; use a new `job_id` for now. |
| `Stage(stage_root=...)` | One `artifacts_root` on `DurableStorage.at`. |
| `Stage(clean_dirs=True)` | Rejected: cleanup could delete committed output. |
| `Step(persist=False)` | Rejected: recovery restores committed output. Use cheap steps plus one `input_scope="all"` step. |
| `keep_source_tag` | **Still supported**, with its original default: the tag is stripped before commit unless you pass `keep_source_tag=True`. |
| Stage-directory output layout | `storage.results(pipeline, job_id=...)` |
| Checkpoint / ownership / lease plugins | Owned by the authoritative store. |
| `context_factory` | **Still supported**, unchanged. |

## Supported Envelope

| Component | Supported | Not supported |
| --- | --- | --- |
| Authoritative progress store | SQLite on a local filesystem that survives invocations. One host, several processes. | Network filesystems, ephemeral disk, multi-host metadata. |
| Committed artifacts | Any fsspec target, including object storage, configured independently via `artifacts_root`. | — |
| Concurrency | Several workers on one host under an explicit `LeasePolicy`. | Multi-host workers (needs a multi-host metadata backend). |
| Branches | Per-partition routing with partition-scoped merge, on one durable engine. | Nested branches; route steps that change partition identity. |
| Remote artifacts | Implemented and locally qualified against an fsspec non-local store. | **Not cloud-credential verified.** No suite has run against real object storage since consolidation. |

## Retries

One `RetryPolicy` type and one `retry` utility cover every boundary: a step, a
batch, a record, or a single operation. Nested boundaries acquire **no** policy
automatically, because nested attempts multiply.

```python
from caravel import RetryPolicy, Step, retry

default_retry = RetryPolicy()      # 3 retries, full-jitter exponential backoff

def retryable(failure):
    error = failure.exception
    return default_retry.should_retry(failure) or (
        isinstance(error, VendorError) and error.code == "TEMPORARILY_BUSY"
    )

vendor_retry = RetryPolicy(when=retryable)

# Retry the whole step scope...
Step(fn=enrich_batch, output=dataset, batch_size=100, retry=vendor_retry)

# ...or just one operation inside the processor.
def enrich_batch(records, context):
    return [retry(lambda: enrich(record), policy=vendor_retry) for record in records]
```

| Setting | Default |
| --- | --- |
| Condition | Recognized transient failures: connection failures, timeouts, rate limits, temporary service failures. |
| Max retries | 3, so at most 4 attempts at the selected boundary. `0` disables retries. |
| Backoff | Uniform sample between zero and `min(30s, 1s * 2^(n-1))`. |
| Result classification | Off. A successful return is accepted unless you pass `on_result`. |

Invalid input, authentication failures, programming errors, and cancellation are
never retried, and arbitrary exception text is never classified as transient.
Retry scope and durable recovery scope are independent: record retries run in
memory within a batch, while the batch stays the durable commit boundary.

## Consuming committed batches

A consumer reads committed references after **its own** durable position, applies
them, and only then acknowledges. Acknowledging never touches producer progress,
and one consumer's failure never blocks another.

```python
from caravel.durability import BatchFeed

feed = BatchFeed(store, pipeline_id=..., job_id=..., node_id=..., output=output)

page = feed.read_for("indexer", limit=100)
for entry in page.entries:
    destination.upsert(entry.partition_id, feed.load(entry, dataset=dataset))
feed.acknowledge("indexer", page.next_position)
```

A crash between application and acknowledgment replays that batch, so
destinations must be idempotent or apply transactionally with the
acknowledgment. The committed manifest is authoritative: missed or duplicate
notifications cannot lose or duplicate a logical update, because consumers catch
up by listing from their position.

An empty page while production is ongoing means "no new batches yet"
(`page.at_end` is `False`). A sealed node reports `page.node_sealed` and its
`final_position`. Reading from below a job's retention floor raises
`RetentionExpiredError` rather than silently skipping expired batches.

A runnable end-to-end example lives in [examples/durable_partitions](examples/durable_partitions).

## Concurrent workers

One worker is the default. Concurrent workers need an explicit lease policy:

```python
from caravel.durability import ExecutionPolicy, LeasePolicy

policy = ExecutionPolicy(workers=4, leases=LeasePolicy(ttl_seconds=60, heartbeat_seconds=10))
```

Workers claim eligible partition-step pairs atomically, so exactly one of two
racing workers wins. Each claim carries an increasing fence token, so a
displaced worker cannot commit even if it is still running, and an expired lease
lets another worker take the unit over. Different partitions advance
concurrently while each partition keeps its dependency order.

The supported envelope is one host with several processes, on a local
filesystem that survives invocations. Multi-host metadata storage needs a
separate backend implementing the same storage contract.

## Guarantees

Processing is at-least-once; immutable attempt output plus conditional progress
publication yields one authoritative committed result per batch. External
effects inside a step still require application-level idempotency, keyed on
something stable such as `(job_id, node_id, partition_id, batch_id)`.

## CLI Usage

`make_cli(pipeline)` drives the same single execution model, so it requires the
same explicit storage decision.

- `--state <dir>` — durable state and artifacts. Requires `--job-id`.
- `--ephemeral` — run without durable state. Nothing resumes; the run reports
  `restart_safe=False`.
- `--job-id <id>` — identity of this bounded run.
- `--param key=value` (repeatable)
- `--graph` — print the Mermaid graph and exit without running.

`--state` and `--ephemeral` are mutually exclusive, and omitting both is an
error rather than a silent default.

```bash
# A resumable run. Invoke it again with the same --job-id to continue.
python3 -m examples.minimal --state data/minimal/state --job-id nightly-2026-11-12

# A throwaway run that makes no durability claim.
python3 -m examples.minimal --ephemeral
```

The run line reports the outcome:

```text
status=completed job_id=nightly-2026-11-12 batches=5 restart_safe=True
```

## Production Status And Support Statement

Caravel is a small deterministic execution core with explicit plugin
capabilities. Guarantees depend on the configured capabilities:

**Bare core** (this package, no plugins) guarantees:

- deterministic compilation, binding, and full-plan execution;
- complete payload validation for built-in datasets before any prior output
  is deleted or overwritten — a validation failure preserves prior output;
- full replacement of Caravel-owned step directories, so a rerun with fewer
  partition keys leaves no stale files;
- in-memory payload flow through full runs, including across stage
  boundaries (stage terminals do not need `persist=True`);
- data-only output directories and no plugin metadata written by bare core;
- fail-closed selective execution: selections needing prior output are
  rejected at plan binding, before user code or mutation; and
- framework logs and errors that name types and identifiers, never payload
  contents, parameter values, or storage-option values. Logging inside your
  own step functions is outside this guarantee.

Bare core does **not** guarantee reuse of existing output, checkpoint-backed
resume, durable run history, cross-run pruning, lease/abandonment evidence, or
a loadable artifact for an empty partitioned output on object stores. These
behaviors require explicitly configured application plugins. Interrupted runs
may leave partial output; bare core never reuses it—a full rerun replaces it.

Caravel is ready to try on real deterministic batch pipelines. Start with the
bare core for the simplest experience, then opt into checkpoints, ownership,
run evidence, and lease diagnostics as your operational needs grow. For a
critical workload, qualify its actual data volume, backend, scheduler, and
rollback path; the included production profile and Azure workflow provide a
concrete starting point.

## Contributing

Please feel free to raise issues, make pull requests, and/or fork this repo!
