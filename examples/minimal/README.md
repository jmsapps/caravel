# Minimal Example

## Purpose

This example demonstrates the linear pipeline flow:

- `Pipeline -> Stage -> Step -> runner` execution order.
- `JSONDataset`-owned persistence at each step.
- Canonical stage/step folder layout under one run root.
- Runtime parameters forwarded through `context.params`.

Pipeline identifiers:

- Pipeline: `minimal_example`
- Stages: `bronze`, `silver`
- Steps: `bronze_collect`, `silver_transform`, `silver_finalize`

## Fixtures

- Input fixture file: `examples/minimal/data/input_partitions.json`
- Loader payload shape: `dict[str, dict]`
- Included fixture keys: `alpha`, `beta`

## CLI Commands

Run these commands from the repo root with Bash or another POSIX-compatible
shell.

### Run The Full Pipeline

```bash
python3 -m examples.minimal --run-root data/minimal_example/smoke_run
```

### Run With A Fresh Output Directory

```bash
rm -rf data/minimal_example/smoke_run
python3 -m examples.minimal --run-root data/minimal_example/smoke_run
```

### Run Only One Stage

```bash
python3 -m examples.minimal --run-root data/minimal_example/smoke_run --stage silver
```

Selective stage execution requires the prior stage output to already exist under
the same `--run-root`.

### Run Only One Step Within A Stage

```bash
python3 -m examples.minimal --run-root data/minimal_example/smoke_run --stage silver --step silver_finalize
```

Selective step execution requires the prior step output to already exist under
the same `--run-root`.

### Pass A Runtime Parameter

```bash
python3 -m examples.minimal --run-root data/minimal_example/smoke_run --param mode=smoke
```

### Emit A Mermaid Graph

```bash
python3 -m examples.minimal --mermaid data/minimal_example/graph.mmd
```

### See All CLI Flags

```bash
python3 -m examples.minimal --help
```

## Where To Inspect Outputs

After running, outputs are written under your `--run-root`.

- Run root: `data/minimal_example/smoke_run/`
- Final step output:
  `data/minimal_example/smoke_run/_002_silver/_002_silver_finalize/_002_silver_finalize.json`

## Smoke Test Flow

1. Run the full pipeline using the first command above.
2. Verify these step output files exist under your `--run-root`:
   - `_001_bronze/_001_bronze_collect/_001_bronze_collect.json`
   - `_002_silver/_001_silver_transform/_001_silver_transform.json`
   - `_002_silver/_002_silver_finalize/_002_silver_finalize.json`
3. Open the final JSON output and confirm summary values and record IDs are
   deterministic.

## Expected Behaviors

- Stages execute in order: `bronze`, then `silver`.
- Steps execute in order: `bronze_collect`, `silver_transform`,
  `silver_finalize`.
- Final output payload fields:
  - `pipeline == "minimal_example"`
  - `count == 2`
  - `ids == ["alpha", "beta"]`
  - `total_normalized == 4.0`
  - `mode == "smoke"` when invoked with `--param mode=smoke`
- Re-running with the same fixture and explicit `--run-root` yields the same
  final JSON values.

## Failure Signals

- Missing fixture path raises `FileNotFoundError` from dataset load.
- Missing expected stage/step paths indicate a stage declaration or runner layout
  regression.
- Unexpected final IDs or totals indicate deterministic transformation behavior
  regressed.
