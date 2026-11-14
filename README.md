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

A pipeline is a name and ordered stages. A step's function is **ordinary
processing code**: it receives one `StepInputs` object and the runtime context,
and only the durable engine ever calls it. `input_scope` on the `Step` declares
whether it runs once per partition or once as an aggregate barrier.

```python
from caravel import DurableStorage, InMemorySource, Pipeline, Stage, Step, run

def load(inputs, *, context):
    records = {}
    for partition in inputs.partitions():
        records[partition.partition_id] = {**partition.load(), "loaded": True}
    return records

def enrich(inputs, *, context):
    records = {}
    for partition in inputs.partitions():
        for key, record in partition.load().items():
            if key in records:
                raise ValueError(f"Duplicate record key: {key}")
            records[key] = {**record, "enriched": True}
    return records

def summarize(inputs, *, context):
    # Handles load on demand, so a barrier over a large job stays bounded.
    return {"documents": sum(len(partition.load()) for partition in inputs.partitions())}

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
the same model. A step runs once per partition unless it declares
`input_scope="all"`.

Scope is compiled from the declaration, before source discovery and before any
durable mutation. No processor runs during planning, and nothing about a
processor's signature, name, annotations, or return value selects an execution
mode. The same function can therefore be declared under either scope: scope
changes readiness and which handles the processor receives, never the type of
what it is handed.

### One input interface, in every scope

Both scopes pass the same `StepInputs` class, yielding the same `InputPartition`
handles:

| Member | Meaning |
| --- | --- |
| `inputs.partitions()` | a fresh iterator of handles; payloads load on demand |
| `inputs.batch_id` | the durable batch this call commits |
| `len(inputs)`, `inputs.is_empty` | how many handles this batch holds |
| `partition.partition_id` | the input's declared identity |
| `partition.batch_id`, `partition.count` | its durable batch and record count |
| `partition.load()` | its payload, in the dataset-defined type |

Under `input_scope="partition"` there is exactly one handle: the unit's own
input, or one batch of it. Under `input_scope="all"` there is one handle per
required upstream commit. An aggregate-derived handle carries the upstream
step's declared output name, so the private aggregate execution ID never
reaches a key your processor produces.

### Storage is an explicit decision

`run()` requires `storage` and never defaults it, because durability is the one
thing a caller must not get by accident.

| Choice | Meaning |
| --- | --- |
| `DurableStorage.at(path)` | Authoritative state under `path`, surviving invocations. Requires an explicit `job_id`. `result.restart_safe` is `True`. |
| `DurableStorage.at(path, artifacts_root="s3://bucket/run")` | Same, with committed artifacts in object storage. The authoritative store stays local. |
| `DurableStorage.ephemeral()` | Explicit opt-out. Same state machine and same commit rules — only the persistence guarantee is lost. `job_id` is optional and `result.restart_safe` is `False`. |

### Reading output

Two surfaces, for two jobs.

**Readable files.** `DurableStorage.at(output)` publishes results named after
the declaration. Execution is durable by default, and recovery storage lives in
`.caravel` beside the output:

```text
data/minimal_example/
├── .caravel/             reserved execution storage — do not write here
│   ├── state.sqlite3     authoritative progress store
│   └── attempts/...      immutable attempt artifacts
└── output/
    └── customer-42/documents/snapshot-2026-11-12/
        ├── 001-prepare/001-load/doc-1/doc-1.json
        ├── 002-publish/001-sync_index/doc-1/doc-1.json
        └── _job.json     completion snapshot (advisory — see below)
```

`.caravel/` is **reserved execution storage** required for recovery: deleting it
loses the ability to resume a job, and nothing but Caravel may write there. It
must be a local filesystem path. Configurations where it would sit inside the
output directory, or vice versa, are rejected — publishing or rebuilding must
never be able to destroy authoritative state.

A hierarchical `pipeline_id` such as `customer-42/documents` becomes nested
directories. Each partition gets its own directory, so ownership is exact:
`doc` and `doc.other` are different partitions and retiring one never touches
the other. Partitioned datasets keep their internal paths, so `en/record_1`
publishes to `en/record_1.json` rather than colliding with the French one.

```python
DurableStorage.at("exports/output")                                  # state beside it
DurableStorage.at("exports/output", state="/persistent/.caravel")    # explicit
DurableStorage.at("s3://bucket/output", state="data/docs/.caravel")  # remote output
DurableStorage.ephemeral("exports/output")                           # results still kept
```

Remote output cannot host the progress store, so pass `state` explicitly for it;
the CLI falls back to `data/<pipeline-name>/.caravel`. `ephemeral` makes only
the *recovery* storage throwaway — published results stay at the same place a
durable run would put them, and closing the storage never deletes them.

**Manifest-aware access**, which is the authority:

```python
storage = DurableStorage.at("state/")
results = storage.results(pipeline, job_id="snapshot-2026-11-12")
results.of("enrich")        # {logical output id: payload}
results.feed("enrich")      # a bounded, ordered consumer feed
```

Output is addressed by **logical identity**, never by execution identity. A
per-partition step keys on each source partition's own id. A step declaring
`input_scope="all"` runs as one work unit on a reserved aggregate partition, but
that id is private scheduling and recovery state: its output is addressed by
the step's declared dataset name, which defaults to the declared step name.

```python
Step(summarize, input_scope="all", output=JSONDataset(name="summary"))
# results.of("summarize")          -> {"summary": {...}}
# published                        -> <stage>/<step>/summary/summary.json

Step(summarize, input_scope="all")  # no dataset name declared
# results.of("summarize")          -> {"summarize": {...}}
```

`input_scope="all"` chooses input availability and readiness. It does not choose
an output name, merge payloads, or repartition: a processor returning a
partitioned payload keeps its returned keys inside that one logical result, and
partition-scope steps downstream of a barrier publish under their own
declarations.

The published tree is a *projection* of committed state, never a second source
of truth. Publication progress lives in the progress store, so the two cannot
drift; deleting the tree loses nothing, and it is rebuilt from the manifest.

A partition appears as soon as it seals, so output is readable while the job
runs.

**Three reader contracts, deliberately distinct.**

| Contract | Use | Guarantee |
| --- | --- | --- |
| Authoritative | `storage.results(...)`, `.published(step)` | Always correct. Reads through committed metadata. |
| Progressive physical | the output tree during a run | Partitions appear as they seal. Superseded generations may still be present; never infer "current" from a directory listing. |
| Whole-job snapshot | `_job.json` | Names the exact file for every logical output at the moment it was written. **Advisory**: presence is not proof of completion. |

**Why concurrent publishers are safe.** Each generation of a logical output
publishes to its own write-once location (`doc-1.json`, then `doc-1@g1.json`).
A publisher that stalls across a recomputation writes its own generation's
files and can never overwrite a newer generation's, whenever it resumes and
however it is interrupted. Write-once holds *within* a generation too: a second
publisher of the same generation leaves the existing files untouched rather
than rewriting them, and an explicit rebuild swaps through a temporary name, so
no reader ever sees a truncated file.

**Retention is a policy, and generations are retained by default.** A manifest
names specific generations, so deleting one breaks a reader holding it.
Pruning is therefore opt-in:

```python
DurableStorage.at("exports/output")                      # retain every generation
DurableStorage.at("exports/output", keep_superseded=1)   # keep the previous one
DurableStorage.at("exports/output", keep_superseded=0)   # keep only the current
```

Pruning preserves the current generation and other partitions. Invalidation
cleanup is bounded by the old generations captured in its transaction, preserving
output published by a concurrent recomputation.

**The completion marker is advisory, and the store is the authority.**
`_job.json` is written last, only once every sealed unit is materialized, and
withdrawn if the job generation moved while it was being written. Invalidation
removes it on both sides of its commit. All of that is best effort: a publisher
that dies between writing the file and re-reading the job leaves a stale marker
behind, and no file-only protocol can prevent it.

So **the marker existing never means a job completed.** Turn it into a
metadata guarantee with `validate_job_manifest(manifest, store)`, or ask the store
directly. Validation compares identities, generations, completeness, and recorded
file references in one transactional snapshot. It establishes currency at that
instant; external deletion or corruption requires `storage.rebuild_output(...)`.

Stores created before publication-reference tracking reject validation for units
without recorded file references. Run `storage.rebuild_output(...)` once to record
their references and regenerate the marker.

```python
from caravel.durability import read_job_manifest, validate_job_manifest

manifest = read_job_manifest("exports/output", "customer-42/documents", "snapshot-1")
if manifest is not None and not validate_job_manifest(manifest, storage.progress):
    ...   # the job is complete and this snapshot is current
```

A reader with no store access gets a consistent snapshot of some past moment.
That is genuinely weaker, and it is the strongest thing a file-only reader can
have.

Promotion is a rename on a local filesystem. On an object store there is no
atomic directory swap, so a reader can observe a partially promoted partition.

Recomputation replaces the logical current result: `storage.invalidate(...)`
removes the completion marker first, then commits, then clears that step's
published output. The next run writes the new generation, and a generation
producing fewer files leaves none of the old ones behind.

To repair a published tree that was deleted or edited outside Caravel, use
`storage.rebuild_output(pipeline, job_id=...)`. Ordinary publication will not
do this: the store correctly believes those units are already published.

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
as a `source`. Source identity remains in durable lineage metadata. Payload tags
are stripped before commit by default; pass `keep_source_tag=True` to retain them.

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
partition's identity. Whole-input processing belongs in an `input_scope="all"`
step outside the branch; returned storage keys do not create new execution partitions.

## Breaking Changes From The Single-Pass Runner

| Removed | Replacement |
| --- | --- |
| `run(pipeline, run_root)` | `run(pipeline, job_id=..., source=..., storage=...)` |
| `only_stage` / `only_step` | Re-invoking a job skips committed work. Use `storage.invalidate(pipeline, job_id=..., step=..., partitions=...)` for selective recomputation; dependent work is invalidated with it. |
| `Stage(stage_root=...)` | One `artifacts_root` on `DurableStorage.at`. |
| `Stage(clean_dirs=True)` | Rejected: cleanup could delete committed output. |
| `@step(...)` decorator | `Step(...)` is the only step configuration surface. Move the decorator's arguments into the `Step(...)` that lists the processor in a stage or branch route: `@step(output=d)` above `def load` becomes `Step(load, output=d)`. A bare callable still works wherever it wants every documented `Step` default. |
| `Step(persist=False)` | Rejected: recovery restores committed output. Use cheap steps plus one `input_scope="all"` step. |
| `keep_source_tag` | **Still supported**, with its original default: the tag is stripped before commit unless you pass `keep_source_tag=True`. |
| Stage-directory output layout | `storage.results(pipeline, job_id=...)` |
| Checkpoint / ownership / lease plugins | Owned by the authoritative store. |
| `context_factory` | **Still supported**, unchanged. |
| `__all__` as a published directory, result key, manifest identity, and feed address | The declared dataset name, defaulting to the declared step name. The reserved aggregate id is private to the progress store and attempt artifacts. |

Logical output identity is a breaking change to the published layout and to the
durable store, which is now schema version 2. A store written under version 1 is
not opened: start a new job against a fresh recovery directory and output root,
or keep the old state on the version that wrote it, rebuild its output there,
and migrate the results. There is no dual-layout mode and no alias tree.

## Supported Envelope

| Component | Supported | Not supported |
| --- | --- | --- |
| Authoritative progress store | SQLite on a local filesystem that survives invocations. One host, several processes. | Network filesystems, ephemeral disk, multi-host metadata. |
| Committed artifacts | Any fsspec target, including object storage, configured independently via `artifacts_root`. | — |
| Concurrency | Several workers on one host under an explicit `LeasePolicy`. | Multi-host workers (needs a multi-host metadata backend). |
| Branches | Per-partition routing with partition-scoped merge, on one durable engine. | Nested branches; route steps that change partition identity. |
| Remote artifacts | Implemented and locally qualified against an fsspec non-local store. | **Not cloud-credential verified.** No suite has run against real object storage since consolidation. |

## Retries

One `RetryOptions` type and one `retry_operation` utility cover every boundary:
a stage, a step, a managed helper, or a single operation. The same instance is
accepted as `retry=` at every one, and nested boundaries acquire **no** options
automatically, because nested attempts multiply.

```python
from caravel import RetryOptions, Stage, Step, retry_operation

default_retry = RetryOptions()      # 4 total attempts, capped exponential backoff

def retryable(error):
    return default_retry.should_retry_error(error) or (
        isinstance(error, VendorError) and error.code == "TEMPORARILY_BUSY"
    )

vendor_retry = RetryOptions(should_retry_error=retryable)

# The same instance at every supported boundary.
Stage(name="prepare", retry=vendor_retry, entries=[
    Step(fn=enrich, output=dataset, batch_size=100, retry=vendor_retry),
])

# ...or just one operation inside the processor. Only that call repeats.
def enrich(inputs, *, context):
    results = []
    for partition in inputs.partitions():
        for record in partition.load():
            value, attempts = retry_operation(
                operation=lambda record=record: call_vendor(record), retry=vendor_retry
            )
            results.append(value)
    return results
```

An ordinary loop iteration acquires no retry and no checkpoint of its own. In
partition scope durable completion stays per partition or batch; in all scope a
failed batch replays that whole batch, so successful iterations inside it may
run again.

Inside a processor, `retry_operation` inherits the invocation's deadline,
cancellation state, ownership, and shared retry allowance automatically — there
is no context argument to wire up. Outside the runner the same function works
with its own local limits and claims no durable recovery.

| Setting | Default |
| --- | --- |
| `should_retry_error` | Recognized transient failures: connection failures, timeouts, rate limits, temporary service failures. |
| `should_retry_result` | Off. A successful return is accepted unless you pass a predicate. |
| `max_attempts` | 4, counting the initial call. `1` permits the initial call without retry; `retry=None` disables the boundary. |
| `delay_seconds` | `compute_retry_delay`: `min(2^(n-1), 16)` seconds plus up to one second of jitter, so at most 17s. |

Every boundary also shares one invocation-wide allowance,
`ExecutionPolicy(max_retry_attempts=...)`, finite at 100 by default. Initial
attempts never consume it; only retries do, so explicitly nested policies cannot
multiply attempts without bound. Exhausting it is terminal for that invocation's
retry chain.

Invalid input, authentication failures, programming errors, and cancellation are
never retried, and arbitrary exception text is never classified as transient.
`KeyboardInterrupt`, `SystemExit`, and other `BaseException` control signals
propagate without retry.

### Migrating from the removed declaration and retry APIs

This is a breaking replacement with no aliases or compatibility dispatch. Old
API usage fails explicitly, and a job whose persisted declaration fingerprint
does not match the current declaration fails before any source read or
mutation; start a new `job_id`.

| Removed | Replacement |
| --- | --- |
| `Inputs`, `inputs.each(...)`, `inputs.all(...)` | a plain processor plus `Step(fn, input_scope=...)` |
| `ManagedDescriptor`, `DeclarationContext` | nothing: scope is declared, and the runtime context is the processor's `context=` |
| `PartitionInput`, `AggregateInput` | one `StepInputs` yielding `InputPartition` handles |
| `ManagedInputError` | `StepInputsError` |
| `inputs.each(callback, retry=...)` | `Step(fn, retry=...)`, or `retry_operation()` inside the processor |
| `RetryPolicy(max_retries=n, when=...)` | `RetryOptions(max_attempts=n + 1, should_retry_error=...)` |
| `RetryPolicy(on_result=...)` | `RetryOptions(should_retry_result=...)` |
| `retry(op, policy=p)` | `retry_operation(operation=op, retry=p)`, returning `(result, attempts)` |
| `FailureContext`, `default_classifier`, `RetryBudget` | Internal. Predicates take a bare exception or result; the shared allowance is `ExecutionPolicy(max_retry_attempts=...)`. |

## Consuming committed batches

A consumer reads committed references after **its own** durable position, applies
them, and only then acknowledges. Acknowledging never touches producer progress,
and one consumer's failure never blocks another.

```python
feed = storage.results(pipeline, job_id=...).feed("publish")

page = feed.read_for("indexer", limit=100)
for entry in page.entries:
    destination.upsert(entry.logical_id, feed.load(entry, dataset=dataset))
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

Start with [minimal](examples/minimal/README.md) for independent per-partition
steps. See [combining_results](examples/combining_results/README.md) for combining
partition outputs into one summary with `input_scope="all"`.

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

`make_cli(pipeline)` drives the same single execution model.

- `--output PATH` — where published results go. Default:
  `data/<pipeline-name>/output`. May be an fsspec URL.
- `--state PATH` — the recovery directory itself. Default: `.caravel` beside
  the output directory, or `data/<pipeline-name>/.caravel` when output is
  remote.
- `--ephemeral` — throwaway recovery storage; published results still go to the
  configured output.
- `--job-id ID` — identity of this bounded run. Required unless `--ephemeral`.
- `--param key=value` (repeatable)
- `--graph` — print the Mermaid graph and exit.

```bash
python3 -m examples.minimal --job-id nightly-2026-11-12

python3 -m examples.minimal \
  --output /exports/minimal/output \
  --job-id nightly-2026-11-12

python3 -m examples.minimal \
  --output /exports/minimal/output \
  --state /persistent/minimal/.caravel \
  --job-id nightly-2026-11-12

python3 -m examples.minimal --ephemeral
```

The run line reports the outcome and where results landed:

```text
status=completed job_id=nightly-2026-11-12 batches=5 restart_safe=True
output=data/minimal_example/output
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
