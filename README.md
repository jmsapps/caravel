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
processing code**: it receives the complete value the previous step produced
and the runtime context, and returns its own complete result.

```python
from caravel import DurabilityOptions, InMemorySource, Pipeline, Stage, Step, run

def load(value, *, context):
    return {key: {**record, "loaded": True} for key, record in value.items()}

def enrich(value, *, context):
    return {key: {**record, "enriched": True} for key, record in value.items()}

def summarize(value, *, context):
    return {"documents": len(value)}

pipeline = Pipeline(
    name="documents",
    stages=[
        Stage(name="prepare", entries=[Step(load), Step(enrich)]),
        Stage(name="summary", entries=[Step(summarize)]),
    ],
    source=InMemorySource({"doc-1": {...}, "doc-2": {...}}),
    durability_options=DurabilityOptions(
        schema_version="v1.0.0", job_id="snapshot-2026-11-12", commit_scope="step"
    ),
)

result = run(pipeline, output="data/documents/output")
```

`Step(load)` is complete: its output defaults to a JSON dataset named for the
processor. No loader stub and no dataset are required.

### One execution model

There is one engine, reached through `run()`. Steps run in declaration order,
each over the complete value its predecessor returned. Iteration, batching,
parallel calls, and API limits inside a step are ordinary code you write.

Nothing about a processor's signature, name, annotations, or return value
selects an execution mode. The declaration decides what runs and in what order,
before any processor is called.

### One value per step

A processor is called `fn(value, context=...)`. The first step receives what the
source loaded; every later step receives what the step before it returned.
A dataset decides how that value is written and read back, so a partitioned
dataset's value is a complete keyed mapping.

### Durability is a declaration

`durability_options` on the `Pipeline` is what makes a run durable.

| Field | Meaning |
| --- | --- |
| `schema_version` | The storage format this declaration was written against. `"v1.0.0"`. |
| `job_id` | The identity this run resumes under. You supply it; Caravel never derives one. |
| `commit_scope` | When transactions commit: `"pipeline"` (default), `"stage"`, or `"step"`. |

Supplying the options adds the recovery database, executor ownership,
snapshots, status rows, and transactions. Omitting them runs the same
processors with the same retry controls, in memory.

```python
Pipeline(..., durability_options=DurabilityOptions(
    schema_version="v1.0.0", job_id="snapshot-1", commit_scope="step"
))
```

Reopening a job restores every committed step and runs only what is left. A new
job id starts fresh, and one pipeline database holds many job ids
independently.

[`examples/durable_partitions`](examples/durable_partitions/README.md) is a
runnable demo: it sleeps and logs per step, takes `--fail-at` to interrupt a
run, and shows committed steps being restored on the next one.

### Reading output

`run()` returns a result describing what the invocation did and where each step
wrote:

```python
result = run(pipeline, output="data/documents/output")

result.pipeline_name    # "documents"
result.job_id           # "snapshot-2026-11-12", or None without durability options
result.output_root      # where generated output goes
result.steps_executed   # steps whose processors ran
result.steps_restored   # steps restored from committed snapshots
result.outputs          # {step name: the directory its dataset wrote}

JSONDataset().load_from(result.outputs["summarize"])
```

Generated output is written through the declared dataset, under a directory per
stage and step. The recovery database lives in `.caravel` beside the output
root:

```text
data/documents/
├── .caravel/             reserved execution storage — do not write here
│   ├── state.sqlite3     runs, stages, steps, and their snapshots
│   └── owner.sqlite3     executor ownership
└── output/
    ├── _001_prepare/_001_load/load.json
    ├── _001_prepare/_002_enrich/enrich.json
    └── _002_summary/_001_summarize/summarize.json
```

`.caravel/` is **reserved execution storage** required for recovery: deleting it
loses the ability to resume a job, and nothing but Caravel may write there. It
must be a local filesystem path. Configurations where it would sit inside the
output directory, or vice versa, are rejected, so writing output can never
destroy authoritative state.

SQLite is the recovery authority. Deleting generated output costs nothing:
reopening the job regenerates it from committed snapshots without rerunning a
processor.

```python
run(pipeline, output="exports/output")                              # state beside it
run(pipeline, output="exports/output", state="/persistent/.caravel")  # explicit
run(pipeline, output="s3://bucket/output", state="data/docs/.caravel")  # remote output
```

Remote output cannot host the recovery database, so pass `state` explicitly for
it; the CLI falls back to `data/<pipeline-name>/.caravel`.


- `./examples/fsspec/data/input_partitions.json`
- `abfs://caravel/input/input_partitions.json`
- `gs://my-bucket/prefix/input_partitions.json`
- `s3://my-bucket/prefix/input_partitions.json`

## Plugins

Two capabilities, because only these extend behaviour the engine does not
already own.

- **Observers** receive lifecycle events after committed transitions
  (`run_started`, `node_started`, `node_completed`, `run_completed`,
  `run_failed`). Each declares `criticality` as `"required"` or
  `"best_effort"`; a required failure fails the run, a best-effort one does
  not. An observer reports what committed; it never changes it.
- **Run guards** wrap one invocation in an external precondition. They enter in
  plugin order and exit in reverse, and teardown is attempted for every guard
  whose startup completed.

```python
run(pipeline, output="data/documents/output", plugins=[MyObserver()])
```

The recovery database owns progress, committed output, and executor ownership,
so evidence is what a plugin contributes.


## Multi-Source Input And Branches

`MultiSourceLoader` tags each record with `__source__` and is accepted directly
as a `source`. Tags are stripped before output is written by default; pass
`keep_source_tag=True` to retain them.

### Branches

A `Branch` routes the value it receives to one of several step chains, then
merges. Every declared route contributes its steps to the declared structure;
the chosen route runs, and the other routes' steps are recorded as `skipped`,
so a stage containing a branch still completes.

Routing happens **at the branch**, so a predicate may read values earlier steps
produced. The decision is committed with the branch step, which means the
predicate is consulted exactly once per job: reopening a job reuses the route it
already chose, and a flapping or externally-dependent predicate cannot re-route
after a crash.

`by="source"` routes on the source tag the incoming value carries, either a
tagged record or a mapping of records that share one tag. A callable predicate
receives the complete value and returns a declared route key.

## Supported Envelope

| Component | Supported | Not supported |
| --- | --- | --- |
| Recovery database | SQLite on a local filesystem that survives invocations. | Network filesystems, ephemeral disk, multi-host metadata. |
| Executors | One executor per recovery database, with sequential step execution. Parallel calls inside a step are your code. | Concurrent executors on one database; multi-host workers. |
| Generated output | Any fsspec target, including object storage. | — |
| Branches | One route per run, with the other routes' steps recorded as skipped. | Nested branches. |
| Recovery granularity | The step. A failure inside a step repeats that step's uncommitted work. | Record- or operation-level recovery. |
| Remote output | Implemented and locally qualified against an fsspec non-local store. | **Not cloud-credential verified.** |


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
    Step(fn=enrich, output=dataset, retry=vendor_retry),
])

# ...or just one operation inside the processor. Only that call repeats.
def enrich(value, *, context):
    results = []
    for record in value.values():
        enriched, attempts = retry_operation(
            operation=lambda record=record: call_vendor(record), retry=vendor_retry
        )
        results.append(enriched)
    return results
```

An ordinary loop iteration acquires no retry of its own. Recovery granularity is
the step, so a failed step repeats its uncommitted work and successful
iterations inside it run again.

`retry_operation` is callable anywhere: inside a processor to retry a smaller
scope, or in ordinary application code. Only the operation it is given
repeats.

| Setting | Default |
| --- | --- |
| `should_retry_error` | Recognized transient failures: connection failures, timeouts, rate limits, temporary service failures. |
| `should_retry_result` | Off. A successful return is accepted unless you pass a predicate. |
| `max_attempts` | 4, counting the initial call. `1` permits the initial call without retry; `retry=None` disables the boundary. |
| `delay_seconds` | `compute_retry_delay`: `min(2^(n-1), 16)` seconds plus up to one second of jitter, so at most 17s. |

Invalid input, authentication failures, programming errors, and cancellation are
never retried, and arbitrary exception text is never classified as transient.
`KeyboardInterrupt`, `SystemExit`, and other `BaseException` control signals
propagate without retry.

## Executor ownership

One executor runs a recovery database at a time, with sequential step
execution. Ownership is a transaction held open on `owner.sqlite3` beside the
database for the executor's lifetime, so the operating system answers the
liveness question: the lock lasts exactly as long as the process holding it,
and a crash releases it.

A second executor on the same database is refused immediately, naming the
database. Two job ids of the same pipeline therefore do not execute
concurrently. Parallel calls inside a step are ordinary code you write.


## Guarantees

- A step's snapshot and its completion status become durable together, so a
  completed step always has a readable result.
- Every successful boundary writes its state under every commit scope;
  `commit_scope` selects only when transactions commit.
- A step retry runs in memory and writes a snapshot when its output is
  accepted. Each attempt receives the original input, isolated from a previous
  attempt's mutations.
- A stage retry restarts the stage, restoring committed steps and rerunning
  discarded work.
- Reopening a job restores its committed steps and regenerates output deleted
  from disk.
- Recovery matches declared stage and step identity and order. Nothing hashes
  processor code, and input data is never inspected.
- Retries do not provide exactly-once external effects. SQLite governs stored
  state; an external effect is the processor's own concern.


## CLI Usage

`make_cli(pipeline)` builds an entry point for one pipeline.

```python
from caravel import make_cli
from .pipeline import build_pipeline

cli = make_cli(build_pipeline())
raise SystemExit(cli())
```

| Flag | Meaning |
| --- | --- |
| `--output PATH` | Where generated output goes. `make_cli(..., default_output=...)` sets the default; the examples put theirs in `examples/<name>/data/output`. May be an fsspec URL. |
| `--state PATH` | The recovery directory. Defaults to `.caravel` beside the output root. |
| `--job-id ID` | The identity this run resumes under. Supplying it makes the run durable. |
| `--commit-scope SCOPE` | `pipeline`, `stage`, or `step`. Default `step`. |
| `--param KEY=VALUE` | A run-scoped parameter, repeatable. Reaches `context.params`. |
| `--graph` | Print the pipeline as a Mermaid graph and exit without running. |

```console
$ python -m examples.minimal --job-id run-1
pipeline=minimal_example job_id=run-1 executed=2 restored=0
output=examples/minimal/data/output

$ python -m examples.minimal --job-id run-1
pipeline=minimal_example job_id=run-1 executed=0 restored=2
output=examples/minimal/data/output
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
