# Minimal example pipeline smoke test

## Purpose

This ST-09 example proves the **linear** pipeline flow only:

- `Step -> Stage -> Pipeline -> runner` execution order.
- `JSONDataset`-owned persistence at each step.
- Canonical stage/step folder layout under one run root.

It intentionally does **not** prove partitioned datasets, multi-source loading, or branching.
Those behaviors are covered in ST-09a, ST-09b, and ST-10.

## Fixtures

- Input fixture file: `src/poc/examples/minimal/data/input_partitions.json`
- Expected loader payload shape: `dict[str, dict]`
- Included fixture keys: `alpha`, `beta`

## CLI commands (PowerShell, run from repo root)

The pipeline is exposed as a runnable Python module via `__main__.py`, which wires
`build_minimal_pipeline()` into the framework's `make_cli` helper. Use the same
shape for local debugging or for hosting (e.g. Function App) — only the surrounding
runtime differs.

### Run the full pipeline

```powershell
python -m examples.minimal --run-root src\poc\data\minimal_example\smoke_run
```

### Run with a fresh output directory (clean re-run)

```powershell
Remove-Item -Recurse -Force src\poc\data\minimal_example\smoke_run -ErrorAction SilentlyContinue
python -m examples.minimal --run-root src\poc\data\minimal_example\smoke_run
```

### Run only one stage (debug a single stage)

```powershell
python -m examples.minimal --run-root src\poc\data\minimal_example\smoke_run --stage silver
```

### Run only one step within a stage (debug a single step)

```powershell
python -m examples.minimal --run-root src\poc\data\minimal_example\smoke_run --stage silver --step silver_finalize
```

### Pass one custom runtime param (`--param key=value`)

```powershell
python -m examples.minimal --run-root src\poc\data\minimal_example\smoke_run --param mode=smoke
```

### See all available CLI flags

```powershell
python -m examples.minimal --help
```

## Where to inspect outputs

After running, outputs are written under your `--run-root`. For the commands above:

- Run root: `src\poc\data\minimal_example\smoke_run\`
- Final step output: `src\poc\data\minimal_example\smoke_run\_002_silver\_002_silver_finalize\_002_silver_finalize.json`

Open the final JSON file in your editor to inspect results manually.

## Smoke test flow

1. Run the full pipeline using the first command above.
2. Verify these step output files exist under your `--run-root`:
   - `_001_bronze/_001_bronze_collect/_001_bronze_collect.json`
   - `_002_silver/_001_silver_transform/_001_silver_transform.json`
   - `_002_silver/_002_silver_finalize/_002_silver_finalize.json`
3. Open the final JSON output and confirm summary values and record IDs are deterministic.

## Expected behaviors

- Stages execute in order: `bronze` then `silver`.
- Steps execute in order: `bronze_collect`, `silver_transform`, `silver_finalize`.
- Final output payload fields:
  - `pipeline == "minimal_example"`
  - `count == 2`
  - `ids == ["alpha", "beta"]`
  - `total_normalized == 4.0`
  - `mode == "smoke"` when invoked with `--param mode=smoke`
- Re-running with the same fixture and explicit `--run-root` yields the same final JSON values.
- Passing `--param mode=smoke` is consumed by `silver_finalize`, and the final
  JSON includes `"mode": "smoke"`.

## Failure signals

- If fixture path is missing, run fails with `FileNotFoundError` from dataset load.
- If any expected stage/step path is missing, stage/step declaration or runner layout regressed.
- If final `ids` ordering/value totals change unexpectedly, deterministic transformation behavior regressed.
