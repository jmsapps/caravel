# ST-10 Branching example

## Purpose

This example proves **ST-10** branch routing + convergence behavior using `Branch`
on top of already-proven dataset/loader primitives.

Specifically, it validates:

- Source-based route selection in bronze (`json_source` and `html_source`).
- Route lineage persisted under route-specific folders.
- Convergence to a uniform silver schema.
- Gold repartitioning with path-style keys (`en/<id>`, `fr/<id>`).

Pipeline identifiers:

- Pipeline: `branching_example`
- Stages: `bronze`, `silver`, `gold`
- Branch entry: `branch_by_source`
- Bronze route steps: `bronze_normalize_json_source`, `bronze_normalize_html_source`
- Silver step: `silver_unify_records`
- Gold step: `gold_partition_by_language`

## Fixtures

Happy-path fixtures:

- `src/poc/examples/branching/data/json_source/json_alpha.json`
- `src/poc/examples/branching/data/json_source/json_beta.json`
- `src/poc/examples/branching/data/html_source/html_alpha.html`
- `src/poc/examples/branching/data/html_source/html_beta.html`

Collision fixtures:

- `src/poc/examples/branching/data_collision/json_source/json_collision.json`
- `src/poc/examples/branching/data_collision/html_source/html_collision.html`

The collision set intentionally shares logical ID `shared-001` across routes to
trigger a branch merge `KeyCollisionError`.

## Smoke test flow

1. Run the pipeline CLI:

   `python -m examples.branching --run-root src\poc\data\branching_example\smoke_run`

2. Verify bronze route lineage files exist:
   - `.../_001_bronze/_001_branch_by_source/json_source/bronze_normalize_json_source/json-alpha.json`
   - `.../_001_bronze/_001_branch_by_source/html_source/bronze_normalize_html_source/html-alpha.json`

3. Verify silver unified records under `.../_002_silver/_001_silver_unify_records/`.

4. Verify gold path-style outputs under `.../_003_gold/_001_gold_partition_by_language/en/*.json`
   and `.../fr/*.json`.

5. Optional regression check:

   `python -m pytest src/poc/examples/tests/test_branching_pipeline.py -q`

## Expected behaviors

- Bronze `Branch(by="source")` routes records by `__source__` into both `json_source`
  and `html_source` route folders.
- Silver output records expose a uniform minimum schema:
  `id`, `language`, `source`, `content`.
- Gold output keys are strictly path-style language keys (`<language>/<id>`).
- Re-running with the same explicit run root yields deterministic output values.
- Collision fixtures fail fast with `KeyCollisionError` mentioning `shared-001`.

## Failure signals

Watch for these fast signals of ST-10 contract drift:

- Missing route lineage folders under `_001_bronze/_001_branch_by_source/{json_source,html_source}`.
- Silver records missing required schema fields.
- Gold outputs not partitioned by language folders (`en/`, `fr/`).
- Collision fixtures not raising `KeyCollisionError` on `shared-001`.
- README missing required runbook sections or ST-10 identifiers.
