# PoC Pipeline Framework

This folder contains the reusable pipeline framework used by the examples under
`src/poc/examples/**`.

## Purpose

Use this framework to declare and run deterministic data pipelines with:

- typed pipeline primitives (`Pipeline`, `Stage`, `Step`, `Branch`)
- dataset-owned I/O (`JSONDataset`, `Partitioned*Dataset`, etc.)
- source-aware loading (`MultiSourceLoader`)
- deterministic on-disk output layout
- optional Mermaid graph rendering

## Core declaration pattern

```python
from pathlib import Path

from pipeline import (
    JSONDataset,
    Pipeline,
    Stage,
    PartitionedJSONDataset,
    PartitionedTextDataset,
    Branch,
    MultiSourceLoader,
    step,
)


@step(output=PartitionedJSONDataset())
def silver_transform(records: dict[str, dict], *, context):
    return records


def build_pipeline() -> Pipeline:
    loader = MultiSourceLoader(
        [
            PartitionedJSONDataset(name="json_source", path=Path("fixtures/json")),
            PartitionedTextDataset(name="html_source", path=Path("fixtures/html"), suffix=".html"),
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

## Add a new step

1. Define a function that accepts the prior step payload as the first positional
   argument and `context` as a keyword-only argument.
2. Attach output persistence with `@step(output=<Dataset>)`.
3. Add the function to the target `Stage(entries=[...])` in declaration order.
4. Add or update pytest coverage for happy path and one failure/edge path.

## Dataset quick reference

| Dataset                   | Typical payload                | Writes                                 |
| ------------------------- | ------------------------------ | -------------------------------------- |
| `JSONDataset`             | Any JSON-serializable object   | Single JSON file in step folder        |
| `PartitionedJSONDataset`  | `dict[str, JSON-serializable]` | One JSON file per partition key        |
| `TextDataset`             | `str`                          | Single text file (configurable suffix) |
| `PartitionedTextDataset`  | `dict[str, str]`               | One text file per partition key        |
| `BytesDataset`            | `bytes`                        | Single binary file                     |
| `PartitionedBytesDataset` | `dict[str, bytes]`             | One binary file per partition key      |

Partition keys may use path-style nesting like `en/record_001` and are resolved
into nested directories.

## Multi-source + branch pattern

- Use `MultiSourceLoader([...])` to combine source loaders.
- Records are tagged with `__source__` by loader composition.
- Route source-specific normalization with `Branch(by="source", routes={...})`.
- Converge branch outputs in the next step to restore one canonical partition map.

## Output folder layout

By default, each run is written under:

- `src/poc/data/<pipeline_name>/<UTC_timestamp>/`

Inside a run root, outputs are stage/step-scoped:

- `_<stage_index>_<stage_name>/_<step_index>_<step_name>/...`

Example:

- `_001_bronze/_001_branch_by_source/...`
- `_002_silver/_001_merge_records/...`
- `_003_gold/_001_partition_by_language/en/*.json`

## CLI usage

Each example entry point wires `make_cli(pipeline)`, so supported options are
consistent:

- `--run-root <path>`
- `--stage <name|index>`
- `--step <name|index>`
- `--param key=value` (repeatable)
- `--keep-source-tag`
- `--mermaid <out_path>`

`--mermaid` is diagram-only mode and is mutually exclusive with execution flags.

## Mermaid output

Use an example CLI entry point to emit a graph:

- `python -m examples.minimal --mermaid test_out.mmd`

The generated content starts with `flowchart TD` and is deterministic for a
fixed pipeline declaration.
