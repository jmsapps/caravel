# ST-09c Multisource example

## Purpose

This example proves **ST-09c** behavior for `MultiSourceLoader` only:

- Composing two named sources (`src_a`, `src_b`) into a single loader input.
- Injecting and validating `__source__` tags in loaded partition records.
- Failing loudly on partition-key collisions (for example `shared_001`).

It intentionally does **not** prove branch routing/convergence.

Pipeline identifiers used by this example:

- Pipeline name: `multisource_example`
- Stage name: `silver`
- Step name: `summarize_by_source`

## Fixtures

Happy-path fixtures (disjoint keys):

- `src/poc/examples/multisource/data/src_a/a_001.json`
- `src/poc/examples/multisource/data/src_a/a_002.json`
- `src/poc/examples/multisource/data/src_b/b_001.json`
- `src/poc/examples/multisource/data/src_b/b_002.json`

Collision fixtures (intentional overlap key):

- `src/poc/examples/multisource/data_collision/src_a/shared_001.json`
- `src/poc/examples/multisource/data_collision/src_b/shared_001.json`

## Smoke test flow

1. Run the example pipeline CLI:

   `python -m examples.multisource --run-root src\poc\data\multisource_example\smoke_run`

2. Open the output summary JSON:

   `src/poc/data/multisource_example/smoke_run/_001_silver/_001_summarize_by_source/_001_summarize_by_source.json`

3. Confirm counts are present for both `src_a` and `src_b` and that `total_records` matches the sum of source counts.

4. (Optional) run tests for deterministic and collision behavior:

   `python -m pytest src/poc/examples/tests/test_multisource_pipeline.py -q`

## Expected behaviors

- The run writes a single JSON output for `summarize_by_source`.
- Output payload shape is:
  - `pipeline`: `multisource_example`
  - `counts_by_source`: includes `src_a` and `src_b`
  - `total_records`: equals `counts_by_source["src_a"] + counts_by_source["src_b"]`
- Re-running with the same explicit run root is value-deterministic.
- Loading collision fixtures raises `KeyCollisionError` and includes `shared_001` in error messaging.

## Failure signals

Look for these indicators of contract regressions:

- `README.md` missing required sections (`Purpose`, `Fixtures`, `Smoke test flow`, `Expected behaviors`, `Failure signals`).
- Missing source names (`src_a`/`src_b`) or pipeline/stage/step identifiers (`multisource_example`, `silver`, `summarize_by_source`).
- Output JSON missing `counts_by_source` or inconsistent `total_records`.
- Collision fixtures loading without a `KeyCollisionError` on `shared_001`.
