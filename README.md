# Caravel: A Lightweight Pipeline Runner

Caravel is a small Python framework for declaring and running deterministic data
pipelines. It supports ordered stages, ordered steps, source-aware loading,
branch routing, dataset-owned persistence, selective reruns, and optional
Mermaid graph output.

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
- selective stage/step execution
- optional Mermaid graph rendering

## Setup

Install the test dependency in your active environment:

```bash
python3 -m pip install -r requirements.txt
```

Run the test suite:

```bash
python3 -m pytest -q
```

## Core Declaration Pattern

```python
from pathlib import Path

from pipeline import (
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
2. Attach output persistence with `@step(output=<Dataset>)`.
3. Add the function to the target `Stage(entries=[...])` in declaration order.
4. Add or update pytest coverage for the happy path and at least one edge path.

## Dataset Quick Reference

| Dataset                   | Typical payload                | Writes                                 |
| ------------------------- | ------------------------------ | -------------------------------------- |
| `JSONDataset`             | Any JSON-serializable object   | Single JSON file in step folder        |
| `PartitionedJSONDataset`  | `dict[str, JSON-serializable]` | One JSON file per partition key        |
| `TextDataset`             | `str`                          | Single text file (configurable suffix) |
| `PartitionedTextDataset`  | `dict[str, str]`               | One text file per partition key        |
| `BytesDataset`            | `bytes`                        | Single binary file                     |
| `PartitionedBytesDataset` | `dict[str, bytes]`             | One binary file per partition key      |

Partition keys may use path-style nesting like `en/record_001`; partitioned
datasets resolve those keys into nested output directories.

## Multi-Source And Branch Pattern

- Use `MultiSourceLoader([...])` to combine named source datasets.
- Loader composition tags dict records with `__source__`.
- Route source-specific normalization with `Branch(by="source", routes={...})`.
- Use a normal step after the branch when downstream selective execution needs a
  stage-terminal step output.

## Output Folder Layout

By default, each run is written under:

```text
data/<pipeline_name>/<UTC_timestamp>/
```

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

Run an example from the repo root:

```bash
python3 -m examples.minimal --run-root data/minimal_example/smoke_run
```

`--mermaid` is diagram-only mode and is mutually exclusive with execution flags:

```bash
python3 -m examples.minimal --mermaid test_out.mmd
```

The generated content starts with `flowchart TD` and is deterministic for a
fixed pipeline declaration.

## Production Status

The framework is currently suitable for local development, examples, smoke tests, and
non-critical internal runs.
