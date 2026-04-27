# Branching Example

## Purpose

This example demonstrates branch routing and convergence behavior using
`Branch` on top of the dataset and loader primitives.

Specifically, it validates:

- Source-based route selection in bronze (`json_source` and `html_source`).
- Route lineage persisted under route-specific folders.
- A stage-terminal convergence step after branch merge.
- A uniform silver schema after branch convergence.
- Gold repartitioning with path-style keys (`en/<id>`, `fr/<id>`).

Pipeline identifiers:

- Pipeline: `branching_example`
- Stages: `bronze`, `silver`, `gold`
- Branch entry: `branch_by_source`
- Bronze route steps: `bronze_normalize_json_source`,
  `bronze_normalize_html_source`
- Bronze convergence step: `bronze_converged_records`
- Silver step: `silver_unify_records`
- Gold step: `gold_partition_by_language`

## Fixtures

Happy-path fixtures:

- `examples/branching/data/json_source/json_alpha.json`
- `examples/branching/data/json_source/json_beta.json`
- `examples/branching/data/html_source/html_alpha.html`
- `examples/branching/data/html_source/html_beta.html`

Collision fixtures:

- `examples/branching/data_collision/json_source/json_collision.json`
- `examples/branching/data_collision/html_source/html_collision.html`

The collision set intentionally shares logical ID `shared-001` across routes to
trigger a branch merge `KeyCollisionError`.

## CLI Commands

Run these commands from the repo root with Bash or another POSIX-compatible
shell.

### Run The Full Pipeline

```bash
python3 -m examples.branching --run-root data/branching_example/smoke_run
```

### Run With A Fresh Output Directory

```bash
rm -rf data/branching_example/smoke_run
python3 -m examples.branching --run-root data/branching_example/smoke_run
```

### Run Only One Stage

```bash
python3 -m examples.branching --run-root data/branching_example/smoke_run --stage silver
```

Selective stage execution uses the previous stage terminal step,
`bronze_converged_records`, as the seed input.

### Emit A Mermaid Graph

```bash
python3 -m examples.branching --mermaid data/branching_example/graph.mmd
```

### Run The Branching Tests

```bash
python3 -m pytest -q tests/examples/test_branching_pipeline.py
```

## Where To Inspect Outputs

After running, outputs are written under your `--run-root`.

Route lineage files:

- `data/branching_example/smoke_run/_001_bronze/_001_branch_by_source/json_source/bronze_normalize_json_source/json-alpha.json`
- `data/branching_example/smoke_run/_001_bronze/_001_branch_by_source/html_source/bronze_normalize_html_source/html-alpha.json`

Converged bronze output:

- `data/branching_example/smoke_run/_001_bronze/_002_bronze_converged_records/`

Downstream outputs:

- `data/branching_example/smoke_run/_002_silver/_001_silver_unify_records/`
- `data/branching_example/smoke_run/_003_gold/_001_gold_partition_by_language/en/`
- `data/branching_example/smoke_run/_003_gold/_001_gold_partition_by_language/fr/`

## Smoke Test Flow

1. Run the pipeline CLI.
2. Verify bronze route lineage files exist for both sources.
3. Verify the `bronze_converged_records` step exists after the branch entry.
4. Verify silver unified records under `_002_silver/_001_silver_unify_records/`.
5. Verify gold path-style outputs under
   `_003_gold/_001_gold_partition_by_language/en/` and `fr/`.

## Expected Behaviors

- Bronze `Branch(by="source")` routes records by `__source__` into both
  `json_source` and `html_source` route folders.
- Branch route outputs merge into one partition map before
  `bronze_converged_records`.
- Silver output records expose a uniform minimum schema: `id`, `language`,
  `source`, `content`.
- Gold output keys are path-style language keys (`<language>/<id>`).
- Re-running with the same explicit run root yields deterministic output values.
- Collision fixtures fail fast with `KeyCollisionError` mentioning `shared-001`.

## Failure Signals

- Missing route lineage folders under
  `_001_bronze/_001_branch_by_source/{json_source,html_source}`.
- Missing `_001_bronze/_002_bronze_converged_records/` output.
- Silver records missing required schema fields.
- Gold outputs not partitioned by language folders (`en/`, `fr/`).
- Collision fixtures not raising `KeyCollisionError` on `shared-001`.
