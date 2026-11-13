# Durable partition execution

Status: proposed. This document defines the target durable execution contract.

## Objective

Allow a logical job to span multiple bounded invocations without repeating all
completed work. For example, a ten-hour job must make progress through successive
invocations limited to one hour each. The same contract applies to minute-scale
limits.

Each invocation reads and processes a bounded subset of the dataset. Committed
batches become available immediately for incremental consumption; consumers may
instead wait for a durable job completion marker and read the full result.

The core abstraction is a batch/cursor interface backed by durable progress and
durable batch output, integrated into the runner. A scheduler,
queue, or caller is responsible for invoking work again when necessary.

## Scope

The execution model supports sequential durable work units within a unified
`Step`, continuation across process termination, safe retries, and an
optional invocation time budget. It exposes committed batches for independent
consumers with their own progress, as well as completed-job consumption. The
default single-partition input preserves simple function-based declarations.

Snapshot and incremental jobs are first-class modes of the same execution model.
Caravel provides durable source-position tracking and replay contracts; source
adapters provide snapshots or change events. It does not resume arbitrary Python
stacks, automatically split unbounded functions, capture database changes itself,
schedule invocations, or guarantee exactly-once external effects.

## Source modes and run boundaries

A stable `pipeline_id` identifies a configured processing lineage. Each bounded
logical run has a `job_id` and may span many invocation IDs. Incremental pipelines
retain their source checkpoints across runs; completing one run does not complete
or terminate the continuing pipeline.

| Source mode | Selected input | Meaning of run completion |
| --- | --- | --- |
| Snapshot | A fixed snapshot or durable manifest of versioned inputs. | All required work for that snapshot is committed. |
| Incremental | A fixed source-position range selected from a replayable change feed. | All required changes in that range are committed. Later changes belong to subsequent runs. |

Incremental mode initially uses bounded micro-batches rather than a continuously
running worker. Persist each run's start and end positions before discovery;
positions may be vectors for sharded sources. Resuming a run preserves those
bounds. New arrivals never silently expand or replace the current run's input.
The source adapter defines range inclusivity, position comparison, and ordering.

Advance the pipeline's completed-range checkpoint only after the bounded run
completes. Discovery positions and per-step progress remain independent. Initially,
incremental ranges execute sequentially within a pipeline lineage, while partitions
inside a range may run concurrently. Range allocation and checkpoint advancement
must be conditional so overlapping invocations cannot allocate conflicting ranges
or skip failed work. Reprocessing uses an explicit replay lineage rather than
resetting a live checkpoint.

An empty live source means `caught_up`, not permanently finished. The caller may
poll again later. A sealed empty range can still execute all-input steps. Source
retention gaps or unavailable historical input cause an explicit error requiring
an application-approved resnapshot or recovery; positions never silently reset.

```mermaid
flowchart TD
    S[Source adapter] --> M{Source mode}
    M -->|Snapshot| V[Pin snapshot identity]
    M -->|Incremental| R[Select durable bounded position range]
    V --> P[Discover versioned partitions]
    R --> P
    P --> E[Run partition steps and all-input barriers]
    E --> C[Commit bounded run completion]
    C -->|Snapshot| D[Done]
    C -->|Incremental| A[Advance pipeline checkpoint]
    A --> N[Select next range on later invocation]
```

### Input versions and change semantics

Snapshot reads retrieve the pinned version or validate its identity before use.
Incremental events identify their source position, stable entity key, operation
(`insert`, `update`, or `delete`), source revision/order token, and replayable
payload reference. Preserve this metadata through derived output and manifests.
If the source cannot replay historical contents, capture them durably at discovery.
An upper position bound alone does not freeze mutable record contents.

A mid-run update becomes a separate event in a later selected range; processing
does not substitute new contents for a pinned input. Source data remains writable:
version pins are not locks. Adapters declare consistency and ordering guarantees,
including whether positions across source shards can be compared.

Destination steps apply updates and tombstones using stable entity keys and
source-aware version checks or equivalent ordered/idempotent application. Older
work must not overwrite a newer value or resurrect a deleted entity. Applications
define derived-record cleanup, such as removing obsolete chunks after an update.
An incremental manifest describes changes; it is not automatically a materialized
full dataset. Snapshot results, change ranges, and materialized destination views
must be identified distinctly. Adapter-specific schemas and business semantics
remain outside Caravel's core.

## Architectural direction

Durable progress, committed batch publication, and crash recovery are core
execution contracts. Correctness under interruption takes priority over preserving
the current runner architecture or public interfaces. Major rewrites of execution,
persistence, planning, or plugin integration are permitted where needed to satisfy
these contracts.

Reuse existing components where they meet the requirements. Compatibility is a
preference, not an acceptance gate: breaking changes must be explicit and include
migration guidance for affected declarations, plugins, datasets, and stored state.
Existing state must either be migrated with validated semantics or rejected with
clear instructions; it must never be silently interpreted under a new contract.

The interfaces below express the target behavior. Implementation boundaries may
change to meet it. Initial topology and delivery limits define release scope.

## Execution model

One logical job has a stable application-supplied `job_id`. Each invocation has a
different invocation ID but resumes the same durable job state. Each
step has a stable node ID within that job. Stable partition IDs identify
independently recoverable input subsets. Progress is durable for each
`(job_id, node_id, partition_id)` pair. Sequential execution is the default.

```mermaid
flowchart LR
    A1[Load partition 1] --> B1[Enrich partition 1] --> C1[Publish partition 1]
    A2[Load partition 2] --> B2[Enrich partition 2] --> C2[Publish partition 2]
    C1 --> G[All-input barrier]
    C2 --> G
    E[Upstream production sealed] --> G
    G --> Z[Aggregate full dataset]
```

Each arrow requires that partition's preceding step to commit, not completion of
other partitions. A single worker may process partition 1 through every step
before starting partition 2; independent partitions may also run concurrently.

### Execution partitions, datasets, and stages

An execution partition is a stable scheduling and recovery identity. Dataset
types define payload serialization and loading independently of that identity.
`JSONDataset` and `TextDataset` can persist each execution partition's output,
just as partitioned dataset types can. A partitioned storage format is not a
prerequisite for durable execution or concurrency.

| Concern | Responsibility |
| --- | --- |
| Execution partition | Identifies independently scheduled input and tracks its step progress. |
| Dataset | Serializes and loads the payload of a committed work unit. |
| Manifest | Maps execution identities to committed output references and dataset metadata. |
| Stage | Groups and orders step declarations across the pipeline. |

For example, a source may expose 100 documents as 100 execution partitions; each
step may write one JSON or text artifact for each document. A single JSON or text
file is one execution partition by default. Explicit source adapters may split
its contents into stable replayable units when the format permits it. Dataset
selection alone performs no automatic record splitting.

Each dataset write targets an isolated work-unit attempt location. Dataset-owned
replacement is confined to that location and preserves other units' artifacts.
Manifest-backed readers reconstruct committed output using each reference's
dataset metadata, including when a payload itself uses a partitioned format.
Storage partition keys inside such a payload and execution partition IDs occupy
distinct namespaces.

Stages are organizational boundaries, with partition dependencies continuing
across stage boundaries. A partition can enter the next stage while others are
still in earlier stages. Whole-run waiting is expressed by `inputs.all(...)`.

```mermaid
flowchart LR
    T[Scheduler or queue] --> I[Invocation]
    I --> R[Caravel runner]
    R --> S[Step executing durable work units]
    S --> B[Read and process one batch]
    B --> O[Durable batch output]
    O --> P[Commit progress]
    P --> S
    S --> Y[Yield or complete]
    Y -->|More work remains| T
    P --> M[(Committed batch manifest)]
    M --> C[Incremental consumer]
    M --> W[Consumer waiting for job completion]
    S <--> D[(Durable job state)]
```

Only a committed progress record advances the cursor. An invocation resumes at
the cursor in the latest committed record, regardless of leftover files from an
interrupted attempt.

Source discovery commits partition identity and replayable input references
atomically with its discovery cursor. Discovered but unfinished partitions remain
durably eligible across invocations; advancing discovery never implies their
processing is complete. Batch progress is tracked independently for each
partition–step pair. Manifest entries include partition identity, and generations
for individual work units do not serialize unrelated partition processing.

## Proposed public contract

Names below are illustrative; implementation must preserve the semantics.

| Component | Responsibility |
| --- | --- |
| `Step` | Declares a processor, output, revision, input scope, and optional batch source/size under one durable execution contract. |
| `BatchSource.read(cursor, context)` | Reads within the pinned snapshot or bounded change range, returning a stable batch and next cursor, explicit range end, or caught-up status. Supports replay. |
| `process(batch, context)` | Processes one batch and returns its output. |
| `BatchSink` | Writes isolated durable batch output and loads output referenced by committed records. |
| `ProgressStore` | Reads state and conditionally commits progress using a generation/version check. |
| `BatchFeed` | Lists committed batch references after a consumer position and exposes node/job completion state. |
| `RetryOptions` and `retry_operation` | Define and execute the same retry contract at a selected stage, step, helper, or operation boundary. |
| `run(..., pipeline_id=..., job_id=..., budget=...)` | Starts or continues a bounded snapshot/incremental run and returns a structured status. |

### Unified step and batching defaults

Every `Step` executes durable work units using the same commit and recovery model.
The default recovery boundary is a partition–step pair; batching
provides finer recovery within that pair.

| Configuration | Work and recovery boundary |
| --- | --- |
| `batch_size=None` (default) | Execute the selected input scope as one durable unit: one partition by default, or the full dataset for an all-input step. |
| `batch_size=1` | Process and commit one source record per batch. |
| `batch_size=N`, for positive integer `N` | Process and commit up to `N` source records per batch. |

Simple function declarations need no batching configuration. A positive batch
size requires a source with explicit bounded reads and replayable cursor semantics;
Caravel validates that capability before execution. The source may return fewer
than the requested maximum records, and a record may itself represent a file,
partition, or other application-defined item. Reject zero, negative, and noninteger
batch sizes. Size limits and cursor advancement must agree on the consumed input;
the runner must not discard records from an oversized source response.

Input without explicit partitions becomes one partition, preserving simple
function declarations. Arbitrary functions are not automatically split and must
fit invocation limits. Partition identity is stable across steps and invocations;
batch size subdivides work within a partition rather than regrouping partitions.
`batch_size` does not enable retries or change their scope. Whole-scope output and
batched output use the same publication contract.

### Input scope and barriers

| Configuration | Readiness and execution |
| --- | --- |
| `inputs.each(callback)` (per-partition) | Run for each partition as soon as its required upstream partition output commits, while other partitions may still be loading. |
| `inputs.all(callback)` | Wait for sealed upstream production and completion of every required upstream partition, then execute once logically over the full committed dataset. |

All-input steps receive a dataset reader or manifest rather than an eagerly
materialized dataset. Optional batching bounds their work within one logical
aggregate operation. Aggregate output becomes one stable partition for subsequent
steps. That partition's identity is private scheduling and recovery state:
published paths, result keys, manifest entries, and feed addresses use the step's
declared dataset name, which defaults to the declared step name. `inputs.all(...)`
selects input availability and readiness; it does not select an output name.
Partition-scoped steps preserve partition identity; arbitrary repartitioning
and partition fan-out require a separate contract.

"All" means the selected snapshot or bounded incremental range, never an endless
stream. For incremental input it contains that range's changes, not the full
current dataset; applications needing a full view supply a materialized reader.

A required failed partition keeps the barrier closed. Sealed empty input opens
the barrier over an empty dataset. Readiness survives restart; waiting holds no
worker open and an invocation yields when no eligible work remains. Job completion
requires sealed production and completion of every required work unit. Committed
sub-batches are readable through the feed, but the next partition-scoped step
waits for completion of its entire upstream partition.

A batch result contains `batch_id`, payload, and `next_cursor`. End of input is a
distinct result, not an empty payload or a null cursor. An empty batch may still
advance the cursor. The framework rejects nonterminal batches that do not advance
progress. Cursors are bounded, serializable application state, never credentials.

The source must honor the selected mode's version and replay contract. Offset
pagination over mutable collections is insufficient without stable membership
and ordering or durable input capture.

Each batch must fit within a single invocation, including source reads, processing,
output persistence, and progress publication. A processor that cannot meet that
bound must use smaller batches or expose additional checkpoints.

Sources must fetch batches without first materializing the full input dataset.
Invocation limits may include a maximum batch count or record count alongside the
time budget. A yielded invocation retains no required in-memory continuation
state; total job size does not determine the producer's working memory.

## Illustrative Python usage

These examples describe the proposed developer experience, not runnable current
Caravel APIs. Imports, constructors, and processor signatures are illustrative.
`application.*` functions and adapters are supplied by the application; Caravel
owns execution and progress. The examples use a generic document index as one
possible destination, without adding domain-specific behavior to the library.

### A simple pipeline and progressive partition output

```python
from caravel import Pipeline, Stage, Step
from application.documents import enrich, load, sync_index

pipeline = Pipeline(
    name="documents",
    stages=[
        Stage(name="prepare", entries=[Step(load), Step(enrich)]),
        Stage(name="publish", entries=[Step(sync_index)]),
    ],
)
```

Every step declares `inputs.each(...)` unless it declares `inputs.all(...)`, and defaults to `batch_size=None`. With a
partitioned source, the default single worker can execute:

```text
partition A: load ✓ → enrich ✓ → sync_index ✓   # visible in destination
partition B: load ✓ → enrich ✗                  # invocation fails

next invocation, same job:
partition A: already complete
partition B: restore load output → enrich → sync_index
partition C: load → enrich → sync_index
```

Without declared partitions, the input is one partition and each entire function
call is a recovery unit. The runner does not infer how to split the function.

### Snapshot execution across short invocations

```python
from caravel import ExecutionBudget, SnapshotInput, run

result = run(
    pipeline,
    pipeline_id="customer-42/documents",
    job_id="snapshot-2026-11-11-v7",
    source=SnapshotInput(
        adapter=document_source,
        snapshot="v7",                  # immutable, adapter-defined identity
    ),
    progress=durable_store,
    output=durable_output,
    budget=ExecutionBudget(seconds=50, shutdown_margin_seconds=5),
)

if result.status == "yielded":
    scheduler.enqueue(job_id=result.job_id)  # application-owned reinvocation
```

The scheduler passes the same job identity and configuration to every invocation,
including after a hard kill that produced no result. Completed work is restored
from durable storage. A changed `v7` input fails validation rather than silently
reading a newer version. Neither storage location may be ephemeral local disk
when invocations run on different hosts.

### Incremental updates use the same pipeline

```python
from caravel import IncrementalInput, run

# Application orchestration resolves one durable active job for this lineage.
# Retry that job until completed; allocate the next job only afterward.
result = run(
    pipeline,
    pipeline_id="customer-42/document-changes",
    job_id=active_job_id,
    source=IncrementalInput(
        adapter=document_changes,
        max_events=1_000,                # bound this run, not the whole stream
    ),
    progress=durable_store,
    output=durable_output,
    budget=invocation_budget,
)

if result.status == "yielded":
    scheduler.enqueue(job_id=result.job_id)
elif result.status == "completed":
    scheduler.request_next_range(pipeline_id=result.pipeline_id)
elif result.status == "caught_up":
    scheduler.poll_later(pipeline_id=result.pipeline_id)
```

Caravel selects and persists the range before processing; retrying its job ID
reuses those exact bounds. A document update arriving afterward is processed in a
later range. Change envelopes carry entity keys, operations, and source revisions
through the same steps. The application's index adapter applies version-aware
upserts/deletes, including cleanup of obsolete derived records. `completed`
finishes one range; `caught_up` means the live source currently has no new range.

### Whole-dataset work is an explicit barrier

```python
def summarize(dataset, context):
    # Reader over committed partition references, not one giant in-memory list.
    return {"records": sum(partition.count for partition in dataset.partitions())}

pipeline = Pipeline(
    name="documents-with-summary",
    stages=[
        Stage(name="prepare", entries=[Step(load), Step(enrich)]),
        Stage(name="publish", entries=[Step(sync_index)]),
        Stage(
            name="summary",
            entries=[Step(summarize)],
        ),
    ],
)

```

Publishing remains progressive. The summary waits for every required partition
and sealed upstream production. For incremental input, it summarizes this run's
change output; a full current-state summary requires an application-provided
materialized view.

### Choose recovery granularity and reuse one retry configuration

```python
from caravel import RetryOptions, Step, retry_operation
from application.errors import VendorError

default_retry = RetryOptions()           # 4 total attempts, backoff + jitter

def retryable(error):
    return (
        default_retry.should_retry_error(error)
        or isinstance(error, VendorError) and error.code == "TEMPORARILY_BUSY"
    )

vendor_retry = RetryOptions(should_retry_error=retryable)

# Retry only a record operation inside the batch. The managed deadline,
# cancellation, and shared allowance are inherited; there is no context to pass.
def enrich_batch(partition, *, context):
    return [
        retry_operation(
            operation=lambda record=record: enrich_record(record), retry=vendor_retry
        )[0]
        for record in partition.load()
    ]

def enrich(inputs, *, context):
    return inputs.each(enrich_batch)      # no outer retry enabled

record_retry_step = Step(enrich, batch_size=100)

# Alternatively, retry the batch as a whole, using a callback without inner retries.
def enrich_without_inner_retry(partition, *, context):
    return [enrich_record(record) for record in partition.load()]

def enrich_batched(inputs, *, context):
    return inputs.each(enrich_without_inner_retry, retry=vendor_retry)

batched_step = Step(enrich_batched, batch_size=100)
per_record_commit_step = Step(enrich_batched, batch_size=1)
```

These are alternative configurations. A batch-enabled step requires a replayable
batch reader over its selected input scope. A batch callback's `partition.load()` returns a collection,
including a one-record collection when size is 1. With size 100, record retry
success remains in memory until the batch commits: termination
can repeat those records. Size 1 gives each record a durable commit boundary.
Applications can also match explicit error text in their own
`should_retry_error` predicate; default classification does not guess from
arbitrary messages.

### Optional parallel workers

```python
from caravel import ExecutionPolicy, LeasePolicy

# Pass this policy to run() only when the backend supports atomic fenced claims.
execution = ExecutionPolicy(
    workers=4,
    leases=LeasePolicy(ttl_seconds=60, heartbeat_seconds=10),
)
```

Workers claim eligible partition–step pairs; different partitions may advance
concurrently while each partition retains dependency order. A stale worker cannot
commit after takeover. Destination writes still require replay-safe/version-aware
semantics. A single-worker-only release rejects this configuration explicitly.

## Durable state and identity

Progress is namespaced by job ID, node ID, and partition ID, outside directories subject to
ordinary step replacement or stage cleanup.

Durable state includes:

- Schema version, pipeline ID, job ID, partition ID, node ID, and generation.
- Ownership identity, claim epoch/fencing token, and applicable lease expiry.
- Source mode, snapshot identity or bounded source-position range, discovery
  position, and pipeline completed-range checkpoint.
- Explicit step revision, pipeline declaration identity, input version/event metadata,
  output location, and an application-supplied fingerprint of relevant parameters.
- Next cursor, committed batch count, and node completion status.
- References to committed batch output, including stable batch IDs and integrity
  information. These may use a durable linked manifest to avoid rewriting an
  ever-growing list on every batch.
- Invocation ID and timestamps for diagnostics.

Code, source, parameters, or output identity mismatches fail before processing or
mutation. Revision checks are explicit; automatic Python code hashing is not a
correctness guarantee. Restarting with a new job ID begins separate work.

### Authoritative storage contract

One authoritative progress store owns the durable execution state machine.
Before runner implementation, define its concrete schema, transition table,
transaction boundaries, isolation, durability settings, and supported deployment
envelope. Every backend implements that same contract.

Validate required capabilities before user execution, cleanup, or durable mutation:
atomic conditional state transitions, generation and fencing validation, durable
reads, and transactional metadata publication. Generic `fsspec` access establishes
none of these capabilities. Reject unsupported metadata backends explicitly;
artifact storage is independently configured and validated for durable immutable
writes and reads.

State must survive host replacement. Local storage qualifies only when its
filesystem survives invocations and supplies the declared transactional guarantees.
Ownership epochs and fencing checks are foundational schema and commit features,
including single-worker deployments. Concurrent scheduling uses those same fields.

| Transition | Atomic authoritative state change |
| --- | --- |
| Register discovery | Publish replayable partition inputs and advance discovery position together. |
| Commit batch | Validate ownership/fence/generation, publish immutable output references, and advance batch progress together. The partition–step remains open. |
| Seal partition–step | Validate exhausted input and committed batches, then seal completion and establish dependent eligibility together. |
| Complete job | Validate sealed production and all required work, seal output manifests, and publish job completion together. For incremental runs, advance the completed-range checkpoint in the same transaction. |

Dependent eligibility is derived from authoritative committed state or maintained
as an index in the sealing transaction. A durable bounded eligible-work query
supports restart without scanning every partition. Successful batch publication
and partition–step sealing are distinct state transitions, even when an
implementation combines them in one transaction after validating end of input.
An all-input barrier becomes eligible only under its input-scope contract.

## Batch commit protocol

1. Read committed state and its generation.
2. Read and process the batch at the committed cursor.
3. Write output to a unique immutable attempt location and verify persistence.
4. In one authoritative transaction, validate current ownership, fencing token,
   applicable lease expiry, and expected generation; publish the batch's manifest
   reference and advance its cursor/progress together.
5. Only after publication, regard the batch as complete and continue.

```mermaid
sequenceDiagram
    participant R as Runner
    participant P as Progress store
    participant S as Batch source
    participant F as Processor
    participant O as Output store
    R->>P: Read cursor and generation
    P-->>R: Committed state
    R->>S: Read batch at cursor
    S-->>R: Batch ID, payload, next cursor
    R->>F: Process batch
    F-->>R: Output
    R->>O: Persist immutable attempt output
    O-->>R: Verified output reference
    R->>P: Commit reference and progress with ownership, fence, generation checks
    P-->>R: New generation
    Note over R,P: Batch is now committed
```

Readers see only output reachable from committed records. Unreferenced attempt
output is invisible and may be garbage-collected later; correctness must not
depend on immediate cleanup. A conflicting invocation must not overwrite a
winner's output or continue using stale progress.

If a commit response is lost, reload authoritative state before retrying. If the
matching commit is visible, continue from it. If its outcome cannot be established,
stop with an explicit uncertain-commit error; do not advance speculatively.

Processing has at-least-once semantics. Immutable output plus conditional progress
publication gives one authoritative committed result per batch. External effects
inside processing require application-level idempotency, using a stable key such
as `(job_id, node_id, partition_id, batch_id)`, or a transaction that includes those effects.

## Uniform retries

Caravel provides one retry policy and one execution mechanism reusable at any
selected boundary: step, batch, record, or individual operation. Declarative
policy attachment and a callable retry utility use the same policy type and
semantics. Developers learn one contract; pipeline objects do not each require a
different set of retry arguments.

A retry repeats the scope to which the policy is attached. A record retry repeats
one record operation; a batch retry repeats its processing scope; an ordinary
step retry repeats its selected uncommitted partition or all-input scope.
Retrying a batched step reloads authoritative
progress and preserves already committed batches. Framework commit publication
must retain its uncertain-outcome reconciliation rules under every policy.

### Policy and defaults

| Setting | Default and contract |
| --- | --- |
| Retry condition | Recognized transient failures: connection failures, timeouts, rate limits, and temporary service failures. |
| Maximum attempts | `max_attempts=4`, counting the initial call: three retries at the selected boundary. `max_attempts=1` permits the initial call without retry; `retry=None` disables the boundary. |
| Backoff | `compute_retry_delay`: capped exponential plus jitter. Before retry number `n` (starting at 1), wait `min(2^(n-1), 16)` seconds plus up to one second, so at most 17 seconds. |
| Maximum elapsed time | Bounded by the enclosing execution budget, plus a shared invocation allowance (`ExecutionPolicy(max_retry_attempts=...)`, 100 by default) that only retries consume. |
| Activation | The default options apply when retries are enabled at a selected boundary. Nested boundaries acquire no automatic retry options. |

Conditions are user-definable predicates over the bare exception.
`should_retry_error` receives the raised exception and `should_retry_result`
receives the returned value. Users can extend the default classification by
calling `RetryOptions().should_retry_error(error)` inside their own predicate,
or replace it outright. SDK-specific status/code extraction uses explicit
adapters; arbitrary exception text is not classified as transient by default.
Successful return values are accepted unless the caller explicitly supplies a
result classifier for APIs that return failures instead of raising exceptions.
Predicate failures propagate as policy errors rather than becoming retryable.

The default classifier excludes invalid input, authentication/authorization
failures, programming errors, cancellation, and process termination. More specific
permanent classifications take precedence over broad transport categories.
Backoff, attempt limits, elapsed-time limits, and conditions are configurable.

### Nesting and deadlines

Inner retries finish before an enclosing boundary receives the failure. Selecting
multiple retry boundaries is explicit: their attempts can multiply. All nested
calls share the invocation deadline and may additionally share an explicit retry
budget limiting retry attempts across the enclosing scope. Exhausting that shared
budget prevents enclosing policies from restarting the exhausted work.

The utility checks cancellation and remaining time before waiting and before each
new attempt. It starts another attempt only when the configured attempt allowance
and shutdown margin fit within the remaining budget. Waits are cancellation-aware.
If more recoverable work remains but invocation time is insufficient, the runner
yields at a durable boundary. Retry exhaustion or a permanent failure surfaces
the final failure and attempt context without advancing uncommitted progress.

### Retry versus recovery

Retry scope and durable recovery scope are independent. Record-level retries can
run in memory within a batch, while the batch remains the durable commit boundary.
If the invocation dies, successful records in an uncommitted batch may run again.
Preserving individual record successes across invocations requires durable record
completion tracking or treating each record as a batch.

Every retried scope must be replay-safe. A timeout can occur after an external
write succeeds; retry policies do not supply exactly-once effects. Failure does
not imply rollback. Diagnostics expose scope identity, attempt count, classified
reason, and scheduled delay without logging payloads, credentials, or raw error
text by default. Automatic retries within an invocation and caller-triggered
invocation retries remain separately observable; local attempt counters alone do
not impose a lifetime retry limit on a job.

## Time budgets and continuation

An optional monotonic deadline includes a configurable shutdown margin. Before
starting another batch, the runner checks that the remaining time covers a
declared maximum batch duration plus the shutdown margin. If it does not, return
`yielded` without starting the batch. Reject a configuration where no batch can
fit within the invocation budget.

This is cooperative: the runner cannot safely interrupt arbitrary user code.
Hard termination remains correct through batch replay, but a repeatedly oversized
batch cannot make progress. Reading the source and publishing end-of-input state
must also respect the budget.

| Result | Meaning | Caller action |
| --- | --- | --- |
| `completed` | All required work and output for this bounded run are committed. | Stop invoking this job; an incremental pipeline may select another range. |
| `caught_up` | No new source range is available; the incremental pipeline remains open. | Poll or invoke again when updates arrive. |
| `yielded` | Work remains; the invocation stopped at a durable boundary. | Invoke again with the same job ID. |
| Exception/error | Processing or persistence failed, or state is incompatible. | Retry recoverable failures with the same job ID; investigate permanent failures. |

Results include the job ID, invocation ID, active node ID, and committed batch
count. A hard kill may produce no result; the caller must still arrange retry.
Reinvoking a completed job returns `completed` without processing batches again.

```mermaid
stateDiagram-v2
    [*] --> LoadState
    LoadState --> Completed: Job already complete
    LoadState --> CheckBudget: Work remains
    CheckBudget --> Yielded: Insufficient time
    CheckBudget --> ProcessBatch: Enough time
    ProcessBatch --> CommitBatch: Output persisted
    CommitBatch --> CheckBudget: Progress committed
    ProcessBatch --> FinalizeUnit: End of selected input scope
    FinalizeUnit --> Completed: Source sealed and all required work committed
    FinalizeUnit --> CheckBudget: Next eligible partition or barrier
    CheckBudget --> Yielded: Waiting for dependencies
    ProcessBatch --> Interrupted: Failure or termination
    CommitBatch --> Interrupted: Failure or termination
    FinalizeUnit --> Interrupted: Failure or termination
    Interrupted --> LoadState: New invocation
    Yielded --> LoadState: New invocation
    Completed --> [*]
```

## Consumer contract

The output is a manifest-backed dataset readable while production is ongoing.
Consumers choose either of these modes for the same committed output:

| Mode | Availability | Completion condition |
| --- | --- | --- |
| Incremental | Read each batch as soon as its output reference is committed. | The consumer reaches the final manifest position after the selected producer scope completes. |
| Whole job | Wait for the durable job completion marker, then read the full committed dataset. | All required pipeline nodes and output publications are committed. |

The feed identifies its job and output node. It exposes stable, ordered commit
positions, batch IDs, partition IDs, source mode/version or change-range metadata,
immutable output references, schema/revision information,
and completion state. Producer cursors and consumer positions are separate: each
consumer stores its own position under its consumer identity. Listing after a
position returns committed references in order with bounded pagination.

An empty feed while production is ongoing means "no new batches yet," not end of
input. Node completion seals that node's final manifest position. Job completion
is published durably after every required node completes and identifies the
sealed output manifests. Consumers choosing whole-job mode wait for that marker,
even if their selected output node completed earlier. Producer completion does
not require consumer acknowledgment.

For incremental pipelines, completion markers seal bounded ranges. Consumers may
follow successive ranges using durable pipeline-scoped positions. Waiting for a
whole job means waiting for one selected range; continuous pipeline completion is
not implied. Change consumers apply the update/delete contract above.

```mermaid
sequenceDiagram
    participant P as Producer
    participant M as Committed manifest
    participant I as Incremental consumer
    participant W as Whole-job consumer
    P->>M: Commit batch A
    I->>M: Read after own position
    M-->>I: Batch A reference
    I->>I: Apply batch and save consumer position
    W->>M: Check job completion
    M-->>W: Still running
    P->>M: Commit remaining batches
    P->>M: Publish job completion and sealed manifests
    W->>M: Read completed dataset
    I->>M: Read remaining batches through final position
```

Incremental consumers apply a batch to their destination before acknowledging it
in their own durable progress. A crash between application and acknowledgment
replays the batch. Destinations must support idempotent writes or transactional
application with acknowledgment: for example, vector upserts by stable document
and chunk IDs, or a data lake transaction keyed by job, node, and batch ID.
Record keys, update/delete semantics, and destination mappings are application
contracts; batch publication alone does not infer them.

The committed manifest is authoritative. Optional notifications may wake
consumers, but missed or duplicate notifications must not lose or duplicate
logical updates. Consumers catch up by listing from their durable position.
Committed references and data remain available for the job's declared retention
period; retention expiry must be explicit to lagging consumers rather than
silently skipping unavailable batches. Consumer failures do not roll back
producer commits or prevent other consumers from progressing.

## Pipeline and output integration

Destination publication, such as index sync, is a normal partition-scoped step.
External consumers may also use the committed feed. Dependency readiness follows
the input-scope contract.

Finalization seals the existing manifest without combining the entire job output
in memory or copying all batches within one invocation.

Step order is preserved within a partition. If embedding fails after enrichment
commits, enrichment output is restored and only uncommitted embedding work repeats.
All-input dependencies follow the barrier contract above. Processor calls must
fit their runtime and memory limits.

Required cross-invocation upstream outputs need durable completion evidence and
loadable artifacts. Configuration validation rejects in-memory-only dependencies.
A yielded result preserves the active work unit and pipeline's incomplete status.

The initial durable topology is ordered stages with partition-scoped and all-input
steps. Cross-invocation execution inside branches
is rejected until branch continuation and merge state have an explicit contract.
Single-invocation branch execution must have explicit supported behavior and
migration guidance if its execution contract changes.

Validate cleanup and path ownership before mutation: `clean_dirs` or output
replacement must preserve active progress and committed batch data. Whole-output
replacement and batch publication have explicit persistence contracts. The runner
and progress store own work-unit commits and node completion; checkpoint reuse
uses the same authoritative evidence.

## Failure and concurrency requirements

| Interruption point | Required recovery |
| --- | --- |
| Before or during processing | Replay the batch at the committed cursor. |
| During output write | Ignore partial uncommitted output and replay. |
| After output persistence, before progress publication | Replay; previous attempt output remains invisible. |
| After progress publication, before receiving acknowledgment | Discover the commit and continue without committing that batch twice. |
| During partition sealing/dependency activation | Recover either the unsealed state or sealed completion with valid dependent eligibility; retry idempotently. |
| During job completion/incremental checkpoint advancement | Recover either the incomplete job with its prior lineage checkpoint or the completed job with its advanced checkpoint. |
| After consumer destination write, before acknowledgment | Replay that consumer's batch safely without changing producer progress. |
| Two invocations read the same generation | At most one conditional commit succeeds; loser reloads or exits with a conflict. |
| State or committed output is corrupt/missing | Fail explicitly; never silently skip work or reset the cursor. |

Single-worker execution is the default. Concurrent workers require an explicit
lease policy for eligible partition–step work: atomic claims, expiry, heartbeat
renewal, takeover, and increasing fencing tokens, using the authoritative storage
contract. Independent
partitions may run concurrently while dependent steps retain partition order.
Lease storage must support the required atomic operations.

Conditional publication and isolated attempt output remain mandatory. External
writes still require idempotency or destination-enforced fencing: a Caravel lease
cannot retract an external write. A release without multi-worker support must
enforce one active worker per job.

## Acceptance criteria

Verification must demonstrate:

1. A job spans several short invocations and produces the same logical output as
   uninterrupted execution, without reprocessing committed batches.
2. Process termination before and after discovery, batch publication, partition
   sealing, dependency activation, job completion, and incremental checkpoint
   advancement preserves the authoritative transaction invariants. Include lost
   acknowledgments, durable reopen, and rejection of stale fenced commits.
3. Concurrent invocations cannot overwrite committed batch output or advance from
   stale state; external-effect idempotency is tested separately where applicable.
4. Budget exhaustion yields before another batch starts; a hard kill replays only
   uncommitted work. Impossible batch/budget configurations are rejected.
5. Empty input, empty advancing batches, invalid nonadvancing cursors, corrupt state,
   and missing committed output have explicit tested outcomes.
6. Revision, parameter, source snapshot, and output-location mismatches are rejected
   before user processing or cleanup.
7. Completed partition–step pairs survive continuation. A partition reaches later
   steps before other partitions finish ingestion. Finalization remains bounded.
8. Cleanup preserves progress and committed batches. Unsupported storage
   capabilities and branch placement fail before execution or mutation. Every
   supported backend proves the authoritative storage contract through real
   transaction and crash tests, including single-worker fencing.
9. Supported ordinary pipelines and selective execution have regression coverage.
   Any changed public or storage contracts have migration guidance and explicit
   compatibility validation; incompatible stored state is never silently reused.
10. An incremental consumer reads committed batches before producer completion;
    a whole-job consumer waits for the job marker and sees the same logical data.
11. Independent consumers resume from their own positions after failures, tolerate
    missed/duplicate notifications, and safely replay destination writes.
12. An empty ongoing feed differs from a sealed empty result; completion markers
    identify final positions and remain discoverable after invocation failure.
13. Batch source reads, feed pagination, and invocation work limits keep processing
    bounded without loading the full dataset. Retention expiry is explicit.
14. The same retry policy works at step, batch, record, and operation boundaries.
    Defaults allow four total attempts, use the specified jittered backoff, and
    distinguish transient from permanent failures with explicit SDK adapters.
15. Custom exception/result predicates, replacement and extension of defaults,
    disabled retries, and predicate errors have verified behavior. Nested policies
    respect shared retry budgets, cancellation, elapsed limits, and invocation
    deadlines without silently enabling additional retry scopes.
16. Record retry exhaustion leaves the batch cursor uncommitted; invocation recovery
    replays that batch safely. Batched step retries preserve committed batches,
    and retries after ambiguous publication reconcile state before further writes.
17. One `Step` type supports the default `batch_size=None` and positive batch sizes.
    Default functions recover at partition–step boundaries; sizes of 1 and N recover at
    record and N-record batch boundaries respectively. Unsupported sources and
    invalid sizes are rejected before processing, and default output is published
    through the same durable commit contract as batched output.
18. Partition and all-input scopes use one `Step` type. Barriers require sealed
    production and all required partitions, survive restart, handle empty input,
    and stay closed on required failures. Aggregate output identity is stable.
19. A pipeline publishes a partition before source completion without an external
    consumer. Multi-worker support verifies lease expiry, takeover, heartbeat
    failure, and rejection of stale fenced commits; otherwise overlapping workers
    are rejected.
20. Snapshot runs reject unavailable or changed pinned input. Incremental runs
    preserve selected bounds across restarts, allocate ranges conditionally, and
    advance the pipeline checkpoint only after successful range completion.
21. Mid-run updates arrive as separate versioned work. Replay, out-of-order writes,
    and tombstones cannot overwrite newer destination state or resurrect deletions.
    Manifest lineage identifies the input versions and operations actually applied.
22. Empty live input returns caught-up status; later arrivals remain processable.
    Retention gaps fail explicitly. Incremental all-input barriers operate over
    bounded ranges, and change output is distinguished from full snapshot output.
23. JSON, text, and partitioned datasets persist execution-unit output with isolated
    replacement, correct manifest metadata, and restart loading. Execution identity
    remains independent of serialization and internal storage partition keys.
24. A partition crosses stage boundaries and publishes before other partitions
    complete earlier stages. Only explicit all-input dependencies impose whole-run
    barriers; dataset types introduce no scheduling barriers.
