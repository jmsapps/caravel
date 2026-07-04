![Caravel Logo](./public/logo.png)

# Caravel: A Lightweight Pipeline Runner

Caravel is a small Python framework for declaring and running deterministic data
pipelines. It supports ordered stages, ordered steps, source-aware loading,
branch routing, dataset-owned persistence, and optional Mermaid graph output.
Checkpoint-backed selective reruns are provided by an optional checkpoint
plugin (in development); bare Caravel deliberately never treats existing output
files as evidence of completed work.

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
- selective execution of slices that need no prior output (checkpoint-backed
  selective reruns arrive with the checkpoint plugin)
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

```python
from pathlib import Path

from caravel import (
    Branch,
    MultiSourceLoader,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Pipeline,
    Stage,
    step,
)


@step(output=PartitionedJSONDataset())
def silver_transform(records: dict[str, dict], *, context):
    return records


def build_pipeline() -> Pipeline:
    loader = MultiSourceLoader(
        [
            PartitionedJSONDataset(name="json_source", path=Path("fixtures/json")),
            PartitionedTextDataset(
                name="html_source",
                path=Path("fixtures/html"),
                suffix=".html",
            ),
        ]
    )

    branch = Branch(
        name="route_by_source",
        by="source",
        routes={
            "json_source": [],
            "html_source": [],
        },
    )

    return Pipeline(
        name="example_pipeline",
        loader=loader,
        stages=[
            Stage(name="bronze", entries=[branch]),
            Stage(name="silver", entries=[silver_transform]),
        ],
    )
```

## Add A New Step

1. Define a function that accepts the prior step payload as the first positional
   argument and `context` as a keyword-only argument.
2. Attach output and persistence policy with `@step(output=<Dataset>, persist=<bool>)`.
   `persist` defaults to `True`.
3. Add the function to the target `Stage(entries=[...])` in declaration order.
4. Add or update pytest coverage for the happy path and at least one edge path.

### Persistence Policy

- `persist=True` (default): Caravel writes the step output to the step folder.
- `persist=False`: Caravel keeps the step output in memory for downstream steps in
  the same run and does not write step files.

Example with five steps where only the final step writes checkpoint files:

```python
@step(output=JSONDataset(), persist=False)
def step_1(payload, *, context): ...

@step(output=JSONDataset(), persist=False)
def step_2(payload, *, context): ...

@step(output=JSONDataset(), persist=False)
def step_3(payload, *, context): ...

@step(output=JSONDataset(), persist=False)
def step_4(payload, *, context): ...

@step(output=JSONDataset(), persist=True)
def step_5(payload, *, context): ...
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

## Multi-Source And Branch Pattern

- Use `MultiSourceLoader([...])` to combine named source datasets.
- Loader composition tags dict records with `__source__`.
- Route source-specific normalization with `Branch(by="source", routes={...})`.
- A step declared after the branch in the same stage receives the merged
  branch output.

## Output Folder Layout

`run_root` is required for execution. The caller chooses the run root path
or URL (for example a local directory, `abfs://...`, `gs://...`, or `s3://...`).

Inside a run root, outputs are stage/step-scoped:

```text
_<stage_index>_<stage_name>/_<step_index>_<step_name>/...
```

Example:

```text
_001_bronze/_001_branch_by_source/...
_001_bronze/_002_bronze_converged_records/...
_002_silver/_001_silver_unify_records/...
_003_gold/_001_gold_partition_by_language/en/*.json
```

## CLI Usage

Each example entry point wires `make_cli(pipeline)`, so supported options are
consistent:

- `--run-root <path>`
- `--stage <name|index>`
- `--step <name|index>`
- `--param key=value` (repeatable)
- `--keep-source-tag`
- `--mermaid <out_path>`

`--run-root` is optional. If omitted, Caravel writes under
`data/output/<pipeline_name>/...`.
When provided, `--run-root` accepts local paths and `fsspec` URL roots.

Selective execution (`--stage`, `--step`) runs only slices that need no prior
output: the first stage, or step 1 of the first stage. Any selection that
would have to load output from a step it does not execute fails closed before
running user code, because bare Caravel has no checkpoint evidence to trust
existing files. Checkpoint-backed selective reruns return with the optional
checkpoint plugin.

Run an example from the repo root:

```bash
python3 -m examples.minimal
```

Explicit root override:

```bash
python3 -m examples.minimal --run-root data/minimal_example/smoke_run
```

Fsspec-backed input example:

```bash
CARAVEL_INPUT_URL=./examples/fsspec/data/input_partitions.json \
python3 -m examples.fsspec --run-root data/fsspec_example/smoke_run
```

Remote run-root example:

```bash
CARAVEL_INPUT_URL=abfs://caravel/input/input_partitions.json \
python3 -m examples.fsspec --run-root abfs://caravel/output/smoke_run
```

`--mermaid` is diagram-only mode and is mutually exclusive with execution flags:

```bash
python3 -m examples.minimal --mermaid test_out.mmd
```

The generated content starts with `flowchart TD` and is deterministic for a
fixed pipeline declaration.

## Production Status And Support Statement

Caravel is being restructured (C-6) into a small deterministic execution core
plus explicit production plugins. Claims are profile-specific:

**Bare core** (this package, no plugins) guarantees:

- deterministic compilation, binding, and full-plan execution;
- complete payload validation for built-in datasets before any prior output
  is deleted or overwritten — a validation failure preserves prior output;
- full replacement of Caravel-owned step directories, so a rerun with fewer
  partition keys leaves no stale files;
- in-memory payload flow through full runs, including across stage
  boundaries (stage terminals do not need `persist=True`);
- data-only output directories and no control-plane metadata: bare core
  never creates the reserved `_000_metadata` namespace, and user-configured
  output or cleanup paths may not resolve into it;
- fail-closed selective execution: selections needing prior output are
  rejected at plan binding, before user code or mutation; and
- framework logs and errors that name types and identifiers, never payload
  contents, parameter values, or storage-option values. Logging inside your
  own step functions is outside this guarantee.

Bare core does **not** guarantee: reuse of existing output, checkpoint-backed
resume, durable run history, cross-run pruning, lease/abandonment evidence,
or a loadable artifact for an empty partitioned output on object stores.
Those arrive as explicit first-party plugins (checkpoints, ownership, run
evidence, lease) qualified as a named production profile. Interrupted runs
may leave partial output; bare core never reuses it — a full rerun replaces
it.

The framework is currently suitable for local development, examples, smoke
tests, and non-critical internal runs.

## Contributing

Please feel free to raise issues, make pull requests, and/or fork this repo!
