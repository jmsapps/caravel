# Multisource Example

## Purpose

This example demonstrates `MultiSourceLoader` behavior:

- Composing two named sources (`src_a`, `src_b`) into one loader input.
- Injecting and validating `__source__` tags in loaded partition records.
- Failing loudly on partition-key collisions, for example `shared_001`.

It intentionally does not prove branch routing or convergence; see
`examples/branching` for that.

Pipeline identifiers:

- Pipeline: `multisource_example`
- Stage: `silver`
- Step: `summarize_by_source`

## Fixtures

Happy-path fixtures with disjoint keys:

- `examples/multisource/data/src_a/a_001.json`
- `examples/multisource/data/src_a/a_002.json`
- `examples/multisource/data/src_b/b_001.json`
- `examples/multisource/data/src_b/b_002.json`

Collision fixtures with intentional overlap:

- `examples/multisource/data_collision/src_a/shared_001.json`
- `examples/multisource/data_collision/src_b/shared_001.json`

## CLI Commands

Run these commands from the repo root with Bash or another POSIX-compatible
shell.

### Run The Full Pipeline

```bash
python3 -m examples.multisource --run-root data/multisource_example/smoke_run
```

### Run With A Fresh Output Directory

```bash
rm -rf data/multisource_example/smoke_run
python3 -m examples.multisource --run-root data/multisource_example/smoke_run
```

### Emit A Mermaid Graph

```bash
python3 -m examples.multisource --mermaid data/multisource_example/graph.mmd
```

### Run The Multisource Tests

```bash
python3 -m pytest -q tests/examples/test_multisource_pipeline.py
```

## Where To Inspect Outputs

After running, open the output summary JSON:

```text
data/multisource_example/smoke_run/_001_silver/_001_summarize_by_source/_001_summarize_by_source.json
```

Confirm counts are present for both `src_a` and `src_b`, and that
`total_records` matches the sum of source counts.

## Smoke Test Flow

1. Run the example pipeline CLI.
2. Open the output summary JSON.
3. Confirm `counts_by_source` includes `src_a` and `src_b`.
4. Confirm `total_records == counts_by_source["src_a"] + counts_by_source["src_b"]`.

## Expected Behaviors

- The run writes a single JSON output for `summarize_by_source`.
- Output payload shape is:
  - `pipeline`: `multisource_example`
  - `counts_by_source`: includes `src_a` and `src_b`
  - `total_records`: equals the sum of source counts
- Re-running with the same explicit run root is value-deterministic.
- Loading collision fixtures raises `KeyCollisionError` and includes
  `shared_001` in the error message.

## Failure Signals

- Output JSON missing `counts_by_source`.
- Output JSON has an inconsistent `total_records`.
- Collision fixtures loading without `KeyCollisionError` on `shared_001`.
- Missing `__source__` tags in loaded records.
