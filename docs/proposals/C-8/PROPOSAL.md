# Durable step execution

Status: implemented. This document defines the execution contract Caravel
provides.

## Objective

Let a job survive interruption without repeating completed work. A run records
each step's complete result durably, so reopening the job restores what it
already finished and executes only what is left.

## Scope

Caravel executes a pipeline's declared steps in order, each over the complete
value the previous step produced. Durability is a property of the declaration:
supplying `durability_options` adds a recovery database that stores every
completed step's output snapshot and status.

Caravel does not resume arbitrary Python stacks, split a processor, capture
database changes, schedule invocations, or guarantee exactly-once external
effects.

## Execution model

A `Pipeline` is a name, an ordered list of `Stage`s, and a source. A `Stage`
holds ordered entries: `Step`s, bare callables, and `Branch`es.

A processor is called `fn(value, context=...)` — one positional argument
carrying the complete upstream value, plus the keyword-only context. The first
step receives what the source loaded. Iteration, batching, parallel calls, and
API limits inside a step are the programmer's code.

The unit of execution and recovery is the step. Storage granularity is the
step, so a failure inside a costly step repeats that step's uncommitted work.

## Declared structure

Structure comes from the declaration alone: no processor runs, no input is
read, and nothing is hashed.

| Kind | Identity |
|---|---|
| Stage | `stage-{i:03d}` |
| Step | `stage-{i:03d}-entry-{j:03d}` |
| Route step | `{entry_id}-route-{r:03d}-step-{s:03d}` |
| Merge | `{entry_id}-merge` |

A rename preserves identity. Reordering or inserting a declaration changes it,
and a resume whose stored structure differs is rejected.

## Durability options

```python
DurabilityOptions(schema_version="v1.0.0", job_id="...", commit_scope="pipeline")
```

`schema_version` states the storage format the declaration was written against.
`job_id` is the identity the run resumes under; the caller supplies it from
whatever source it has, and Caravel never derives or generates one.
`commit_scope` selects when transactions commit.

Omitting the options runs the same ordered processors with the same retry
controls, in memory.

## Durable state

One SQLite database per pipeline holds many job ids. Four tables carry the
whole of durable state: `durability_metadata`, `runs`, `stages`, and `steps`.
A step row stores its complete output snapshot, the identity of the codec that
encoded it, and its status. The complete schema is in the task record's
[v1 SQL contract](../../../.skills/tasks/C-8-durable-partition-execution/references/v1-sql-contract.md).

A step's snapshot and its completion status become durable together. A step is
`pending`, `running`, `completed`, `skipped`, or `failed`; nothing records
position inside a processor.

A stage completes when every declared step in it is `completed` or `skipped`.
The run completes when every declared stage completes.

## Commit timing

With durability enabled, every successful boundary writes its rows under every
commit scope. `commit_scope` selects only when transactions commit:

| Scope | Commit requested on |
|---|---|
| `pipeline` | successful run completion |
| `stage` | each successful stage, and run completion |
| `step` | each successful step, each successful stage, and run completion |

A commit request persists all pending changes and is skipped when nothing is
pending. After an intermediate commit, a new transaction begins for the
remaining work.

## Retries

A step's in-memory retry loop finishes before any snapshot or status is
written, so a snapshot exists exactly for a step whose output was accepted.
Each attempt receives the original input, isolated from a previous attempt's
mutations.

A stage retry restarts the stage: steps with committed snapshots are restored
and reused, and unfinished steps run again. A stage-start savepoint discards a
failed attempt's uncommitted writes and is recreated after each commit.

`RetryOptions` is one typed contract usable at the step boundary, the stage
boundary, and inside a processor through `retry_operation`. Retries do not
provide exactly-once external effects.

## Branches

A branch routes the value it receives to one declared route, then merges.
Exactly one route executes per run; the other routes' declared steps are
recorded as `skipped`. The routing decision is committed with the branch step,
so reopening a job reuses the route it already chose.

## Executor ownership

One executor runs a recovery database at a time, with sequential step
execution. Ownership is a transaction held open on a sibling `owner.sqlite3`
for the executor's lifetime, so the operating system releases it when the
process ends. A second executor is refused immediately. Two job ids of the same
pipeline therefore do not execute concurrently.

## Recovery

Recovery validates the declared structure against the stored rows, loads
committed step snapshots, restores them to downstream processors, regenerates
generated output through `Dataset.save()`, and executes the unfinished steps in
declared order.

Recovery is keyed on job id and declared structure. Input data is never
inspected or compared, so a reused job id with different data still restores
its completed steps. A new job id starts fresh.

SQLite is the recovery authority: deleting generated output forces no
recomputation.

## Snapshot codecs

A dataset may declare a codec — an identity string plus an encode/decode pair.
`json/utf-8` is the default for the JSON and text datasets; a dataset holding
values JSON cannot carry declares one explicitly. The identity is stored beside
each payload and decoding uses the stored identity, so a renamed or withdrawn
codec is reported rather than guessed.

## Storage format

Storage is `v1.0.0`. A database whose format identity or version differs is
rejected and left in place; recreating a development database is an explicit
action. Version strings are `vMAJOR.MINOR.PATCH`, and a major increment denotes
a breaking format change.

## Acceptance criteria

- A declaration states its processors, its datasets, and its retries; a run
  states its output location.
- Supplying `durability_options` creates the recovery database; omitting them
  runs the same processors in memory.
- Every supported dataset type round-trips through the same snapshot
  representation in the same tables.
- Every successful boundary writes its state under every commit scope, and
  commit requests follow the configured scope.
- A commit request with nothing pending performs no commit; a status-only
  change made after a commit is persisted at the next boundary.
- Reopening a job restores its committed steps, runs the unfinished ones, and
  regenerates output deleted from disk.
- A stage retry restores committed steps and reruns discarded work.
- A branch runs one route and records the other routes' steps as skipped.
- A second executor on one recovery database is refused, naming the database.
- Several job ids coexist independently in one pipeline database.

## Future scope

Manual commits, record- and operation-level recovery, CRUD, typed record
schemas, record keys, snapshot pinning, incremental range tracking, and
cross-job incremental ingestion. The explicit schema version carries that
granularity forward.
