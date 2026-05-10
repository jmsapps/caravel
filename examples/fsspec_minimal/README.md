# Fsspec Example

## Purpose

This example reuses the `examples/minimal` stage/step flow while loading input
through a dataset path that can be either local or a remote URL supported by
`fsspec`.

Pipeline identifiers:

- Pipeline: `fsspec_example`
- Stages: `bronze`, `silver`
- Steps: `bronze_collect`, `silver_transform`, `silver_finalize`

## Fixtures

- Default input fixture:
  `examples/fsspec/data/input_partitions.json`
- Loader payload shape: `dict[str, dict]`
- Included fixture keys: `alpha`, `beta`

## Environment

Set `CARAVEL_INPUT_URL` to choose input source:

- Local:
  `CARAVEL_INPUT_URL=./examples/fsspec/data/input_partitions.json`
- Azure HNS:
  `CARAVEL_INPUT_URL=abfs://caravel/input/input_partitions.json`

For Azure with `abfs://` or `az://`, the example reads auth settings from env
variables, including:

- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_TOKEN`
- Managed identity/service principal fields when applicable

Use `--run-root` for output root selection:

- Local run root:
  `--run-root data/fsspec_example/smoke_run`
- Remote run root:
  `--run-root abfs://caravel/output/smoke_run`

## CLI Commands

Run from repo root.

```bash
python3 -m examples.fsspec --run-root data/fsspec_example/smoke_run
```

With local input URL:

```bash
CARAVEL_INPUT_URL=./examples/fsspec/data/input_partitions.json \
python3 -m examples.fsspec --run-root data/fsspec_example/smoke_run
```

With Azure input URL:

```bash
CARAVEL_INPUT_URL=abfs://caravel/input/input_partitions.json \
AZURE_STORAGE_ACCOUNT_NAME=jmsappsstgdev001 \
AZURE_STORAGE_ACCOUNT_KEY=<key> \
python3 -m examples.fsspec --run-root data/fsspec_example/smoke_run
```

With Azure input + Azure run root:

```bash
CARAVEL_INPUT_URL=abfs://caravel/input/input_partitions.json \
AZURE_STORAGE_ACCOUNT_NAME=jmsappsstgdev001 \
AZURE_STORAGE_ACCOUNT_KEY=<key> \
python3 -m examples.fsspec --run-root abfs://caravel/output/smoke_run
```

## Expected Behaviors

- Stage and step output layout is identical to `examples/minimal`.
- Final output is deterministic for equivalent input payloads.
- Missing input URL/path raises `FileNotFoundError` from dataset load.
