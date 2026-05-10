# Fsspec Partitioned Example

## Purpose

This example combines partitioned input/output with `fsspec` paths and Stage-level
output roots.

It is intended to validate:

- partitioned loader input from local or remote URLs
- optional global `run_root`
- per-stage `stage_root` overrides
- `Stage(clean_dirs=True|False)` behavior

Pipeline identifiers:

- Pipeline: `fsspec_partitioned_example`
- Stages: `bronze`, `silver`, `gold`
- Steps: `bronze_render_html`, `silver_extract_structured`, `gold_partition_by_language`

## Fixtures

- Default input fixture directory:
  `examples/fsspec_partitioned/data/input_partitions`
- Input format: one JSON file per key (`alpha.json`, `beta.json`, `gamma.json`)

## Environment

Core:

- `CARAVEL_INPUT_DIR_URL`:
  local dir or remote URL for partitioned seed input
- `CARAVEL_RUN_ROOT`:
  optional global run root fallback

Per-stage overrides:

- `CARAVEL_BRONZE_STAGE_ROOT`
- `CARAVEL_SILVER_STAGE_ROOT`
- `CARAVEL_GOLD_STAGE_ROOT`

Per-stage cleanup flags:

- `CARAVEL_BRONZE_CLEAN_DIRS`
- `CARAVEL_SILVER_CLEAN_DIRS`
- `CARAVEL_GOLD_CLEAN_DIRS`

Azure auth for `abfs://` or `az://`:

- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_TOKEN`
- optional managed identity/service principal fields

## CLI Commands

Run from repo root with defaults:

```bash
python3 -m examples.fsspec_partitioned
```

Explicit local run root:

```bash
python3 -m examples.fsspec_partitioned --run-root data/fsspec_partitioned_example/smoke_run
```

Local input URL from env:

```bash
CARAVEL_INPUT_DIR_URL=./examples/fsspec_partitioned/data/input_partitions \
python3 -m examples.fsspec_partitioned --run-root data/fsspec_partitioned_example/smoke_run
```

Azure input + per-stage container roots:

```bash
CARAVEL_INPUT_DIR_URL=abfs://caravel/input_partitions \
CARAVEL_BRONZE_STAGE_ROOT=abfs://caravel-bronze/runs/smoke_run \
CARAVEL_SILVER_STAGE_ROOT=abfs://caravel-silver/runs/smoke_run \
CARAVEL_GOLD_STAGE_ROOT=abfs://caravel-gold/runs/smoke_run \
AZURE_STORAGE_ACCOUNT_NAME=jmsappsstgdev001 \
AZURE_STORAGE_ACCOUNT_KEY=<key> \
python3 -m examples.fsspec_partitioned
```

Enable stage cleanup for bronze:

```bash
CARAVEL_BRONZE_CLEAN_DIRS=true \
python3 -m examples.fsspec_partitioned --run-root data/fsspec_partitioned_example/smoke_run
```

## Expected Behaviors

- Without stage overrides, outputs are written under:
  `data/output/fsspec_partitioned_example/_NNN_stage/...`
  or under `--run-root/<pipeline>/_NNN_stage/...` when provided.
- With stage overrides, each stage writes directly under its configured stage
  root and keeps deterministic step folder naming under that base.
- With `clean_dirs=true` for a stage, prior stage-base contents are removed
  before that stage executes.

