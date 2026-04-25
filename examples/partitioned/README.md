# Partitioned example pipeline smoke test

## Purpose

This ST-09b example proves partitioned dataset behavior with canonical runner layout:

- `PartitionedTextDataset` output in bronze (`.html` partitions).
- `PartitionedJSONDataset` output in silver (flat partition keys).
- Path-style key nesting in gold (`en/<id>`, `fr/<id>`).

It intentionally does **not** prove multi-source loading or branch routing.
Those are covered in ST-09c and ST-10.

## Fixtures

- Input fixture folder: `src/poc/examples/partitioned/data/input_partitions/`
- Loader payload shape: `dict[str, dict]` from `PartitionedJSONDataset`
- Included fixture keys: `alpha`, `beta`, `gamma`

## CLI commands (PowerShell, run from repo root)

The example is runnable as a Python module via `__main__.py`, wiring
`build_partitioned_pipeline()` into `make_cli`.

### Run the full pipeline

```powershell
python -m examples.partitioned --run-root src\poc\data\partitioned_example\smoke_run
```

### Run with a fresh output directory (clean re-run)

```powershell
Remove-Item -Recurse -Force src\poc\data\partitioned_example\smoke_run -ErrorAction SilentlyContinue
python -m examples.partitioned --run-root src\poc\data\partitioned_example\smoke_run
```

### Run only one stage

```powershell
python -m examples.partitioned --run-root src\poc\data\partitioned_example\smoke_run --stage gold
```

### Run only one step within a stage

```powershell
python -m examples.partitioned --run-root src\poc\data\partitioned_example\smoke_run --stage gold --step gold_partition_by_language
```

### Pass one custom runtime param (`--param key=value`)

```powershell
python -m examples.partitioned --run-root src\poc\data\partitioned_example\smoke_run --param mode=smoke
```

### See all available CLI flags

```powershell
python -m examples.partitioned --help
```

## Where to inspect outputs

After running, outputs are written under your `--run-root`.

- Bronze output folder:
  `src\poc\data\partitioned_example\smoke_run\_001_bronze\_001_bronze_render_html\`
- Silver output folder:
  `src\poc\data\partitioned_example\smoke_run\_002_silver\_001_silver_extract_structured\`
- Gold output folder (nested language paths):
  `src\poc\data\partitioned_example\smoke_run\_003_gold\_001_gold_partition_by_language\`

Example nested gold files:

- `...\en\alpha.json`
- `...\fr\beta.json`

## Smoke test flow

1. Run the full pipeline using the command above.
2. Verify bronze contains `.html` partition files for `alpha`, `beta`, and `gamma`.
3. Verify silver contains flat `.json` partition files for the same keys.
4. Verify gold contains nested language output paths (`en/...`, `fr/...`).
5. Open one silver file and one gold file to confirm JSON is parseable.

## Expected behaviors

- Stages execute in order: `bronze` -> `silver` -> `gold`.
- Bronze writes text partitions as `.html` files.
- Silver writes JSON partitions using flat keys.
- Gold writes JSON partitions using path-style keys that produce nested folders.
- Re-running with same fixtures and explicit `--run-root` is value-deterministic.

## Failure signals

- Missing fixture directory/file raises `FileNotFoundError` on loader dataset read.
- Missing expected stage/step folders indicates runner layout regression.
- Missing `en/` or `fr/` nested gold files indicates path-style key regression.
- Invalid JSON in silver/gold outputs indicates dataset serialization regression.
