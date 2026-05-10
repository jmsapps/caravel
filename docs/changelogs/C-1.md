# C-1: Fsspec Storage Integration + Remote Run-Root Support

## Summary

This change set adds two major capabilities while preserving existing local behavior:

1. Dataset-level `fsspec` support for local and cloud URL paths (`abfs://`, `az://`, `gs://`, `s3://`, `memory://`).
2. Runner-level remote `run_root` support so stage/step outputs can be written to URL roots, not only local `Path` directories.

The implementation keeps all prior local tests and output layout conventions intact.

## Scope Implemented

- Dataset I/O abstraction over `fsspec` (`load/save/exists` for all dataset classes).
- Optional cloud extras and dependency wiring.
- New `examples/fsspec` pipeline based on `examples/minimal`.
- CLI and runner changes for URL-style `--run-root`.
- Type contract updates for mixed local/remote run paths (`Path | str`).
- Regression coverage for both local and `memory://` remote run roots.

## Dependency and Packaging Changes

File: `pyproject.toml`

- Added base dependency:
  - `fsspec`
  - `python-dotenv==1.2.2`
- Added optional extras:
  - `azure = ["adlfs", "azure-identity"]`
  - `gcp = ["gcsfs"]`
  - `s3 = ["s3fs"]`
  - `cloud` umbrella extra with all three backends

Practical implication:

- `abfs://` usage requires `adlfs` (`pip install -e ".[azure]"`).

## Dataset Layer Refactor (Phase 1)

### New storage helper module

File: `src/caravel/storage.py`

Introduced reusable helpers to avoid backend-specific logic in dataset classes:

- `is_url_path`, `coerce_optional_storage_path`, `ensure_storage_path_set`
- `resolve_fs` (`fsspec.core.url_to_fs`)
- `join_path`, `parent_path`, `leaf_name`, `single_output_path`
- `ensure_parent_dir`, `is_file`, `is_dir`
- `iter_files_with_suffix`, `relative_key_from_file`

### Dataset behavior changes

File: `src/caravel/datasets.py`

All dataset classes moved from direct `pathlib` operations to storage-helper operations:

- `JSONDataset`
- `PartitionedJSONDataset`
- `TextDataset`
- `PartitionedTextDataset`
- `BytesDataset`
- `PartitionedBytesDataset`

Key behavior updates:

- `path` now preserves URL strings (no forced `Path(...)` for `abfs://...`).
- `save` and `exists` accept `Path | str`.
- `storage_options` is supported on constructors for backend-specific auth/config.
- `describe()` now includes:
  - `storage_options_configured: bool`
  - and still avoids exposing secret values.

Preserved behavior:

- Local file naming conventions stayed the same.
- Partition-key validation and payload type checks stayed intact.
- Existing local-path tests remained green.

## Example Pipeline Added

Directory: `examples/fsspec/`

Files added:

- `examples/fsspec/pipeline.py`
- `examples/fsspec/__main__.py`
- `examples/fsspec/__init__.py`
- `examples/fsspec/README.md`
- `examples/fsspec/.env.example`
- `examples/fsspec/data/input_partitions.json`

Characteristics:

- Pipeline flow mirrors `examples/minimal` (`bronze_collect`, `silver_transform`, `silver_finalize`).
- Input dataset path is configurable with `CARAVEL_INPUT_URL`.
- Azure auth options are resolved from env vars in the example.
- `load_dotenv()` is used in the example so root `.env` values are available in module execution contexts.

## Runner + CLI Remote Run-Root Support (Phase 2)

### CLI run-root parsing

File: `src/caravel/cli.py`

- `RunFn` widened to `Callable[..., Path | str]`.
- `--run-root` handling now preserves URL roots as strings:
  - if URL-like (`is_url_path(...)`) -> keep `str`
  - else -> keep existing `Path(...)` conversion

### Runner path model

File: `src/caravel/runner.py`

Key additions:

- `RunPath = Path | str`
- `_is_remote_path`, `_join_run_path`, `_run_path_name`
- `_dataset_at_path` updated to bind step output paths for both local and URL roots.

Core changes:

- `run(...)` now returns `RunPath`.
- Local `run_root` behavior unchanged (`Path`, `mkdir`, local stage/step directories).
- URL `run_root` behavior enabled:
  - stage/step/branch path construction uses storage-aware string joining.
  - selective rerun prior-output lookups use URL paths consistently.
  - run id extraction for `StepContext` uses `_run_path_name(...)`.

### Type contract updates

File: `src/caravel/types.py`

- `StepContext` fields widened:
  - `run_root: Path | str`
  - `step_dir: Path | str`
  - `prev_step_dir: Path | str | None`
- `Dataset` protocol updated:
  - `save(..., dest: Path | str)`
  - `exists(dest: Path | str)`

## Return-Type Alignment in Example Helpers

After runner return shape changed, example wrappers were updated to match:

- `examples/fsspec/pipeline.py`
- `examples/minimal/pipeline.py`
- `examples/partitioned/pipeline.py`
- `examples/branching/pipeline.py`
- `examples/multisource/pipeline.py`

Each `run_*_pipeline(...)` return annotation is now `Path | str`.

## Test Coverage Added/Updated

### New tests

- `tests/caravel/test_storage.py`
  - helper coverage for URL detection, path coercion, suffix listing, relative key derivation.

- `tests/examples/test_fsspec_pipeline.py`
  - validates fsspec example pipeline layout and output values.
  - explicitly clears env vars in tests to avoid accidental external Azure dependency during local/CI runs.

### Extended tests

- `tests/caravel/test_datasets.py`
  - added `memory://` round trips for all six dataset classes.

- `tests/caravel/test_runner.py`
  - added remote run-root cases with `memory://`:
    - full run output persistence
    - selective stage rerun with remote prior load
    - selective step rerun with remote prior load
    - missing remote prior output error behavior

- `tests/caravel/test_cli.py`
  - added assertion that URL run roots are forwarded as `str`.

- `tests/caravel/test_types.py`
  - added `StepContext` assertion for string-based run/step paths.

## Documentation Updates

- `README.md`
  - added cloud install extras.
  - added `fsspec` dataset path examples.
  - added remote `--run-root` example.

- `examples/fsspec/README.md`
  - clarified local and remote run-root usage.
  - documented Azure input + Azure run-root command path.

## Verified Commands and Outcomes

All executed in project virtual environment:

- `.venv/bin/pytest -q tests/caravel/test_storage.py` -> pass
- `.venv/bin/pytest -q tests/caravel/test_datasets.py` -> pass
- `.venv/bin/pytest -q tests/examples/test_fsspec_pipeline.py` -> pass
- `.venv/bin/pytest -q tests/caravel/test_runner.py` -> pass
- `.venv/bin/pytest -q tests/caravel/test_cli.py` -> pass
- `.venv/bin/pytest -q tests/caravel/test_types.py` -> pass
- `.venv/bin/pytest -q` -> `152 passed`

## Runtime Notes

- Using `CARAVEL_INPUT_URL=abfs://...` requires `adlfs` and valid Azure auth env.
- `CARAVEL_INPUT_URL=abfs://caravel/input` fails with `JSONDataset` because that dataset expects a single file path. Use:
  - `abfs://caravel/input/input_partitions.json`
  - or switch the loader to a partitioned dataset if directory semantics are desired.
- Remote output root is enabled through `run_root` / `--run-root` URL values.
- `CARAVEL_OUTPUT_URL` is not auto-bound by runner/CLI; pass output root explicitly.

## File Index (Primary Touchpoints)

- `pyproject.toml`
- `src/caravel/storage.py`
- `src/caravel/datasets.py`
- `src/caravel/runner.py`
- `src/caravel/cli.py`
- `src/caravel/types.py`
- `examples/fsspec/*`
- `tests/caravel/test_storage.py`
- `tests/caravel/test_datasets.py`
- `tests/caravel/test_runner.py`
- `tests/caravel/test_cli.py`
- `tests/caravel/test_types.py`
- `tests/examples/test_fsspec_pipeline.py`
- `README.md`

## Addendum (Phase 4 + Follow-Up Fixes)

This addendum captures the additional behavior implemented after the initial
`fsspec` + remote run-root work.

### 1) Optional `run_root` default restored

Files:

- `src/caravel/paths.py`
- `src/caravel/runner.py`
- `src/caravel/cli.py`

Changes:

- `run_root` is optional again.
- Omitted `run_root` resolves to `data/output`.
- CLI `--run-root` is optional and forwards `None` to the runner when omitted.

Effective default output layout (when no stage override is set):

- `<run_root>/<pipeline_name>/_<NNN>_<stage_name>/_<NNN>_<step_name>/...`

Code example:

```python
# src/caravel/paths.py
def resolve_run_root(override: Path | str | None = None) -> Path:
    if override is None:
        return Path("data/output")
    return Path(override)

# src/caravel/cli.py
if args.run_root is None:
    run_root = None
elif is_url_path(args.run_root):
    run_root = args.run_root
else:
    run_root = Path(args.run_root)
```

### 2) Stage-level root overrides and cleanup policy

Files:

- `src/caravel/pipeline.py`
- `src/caravel/runner.py`

Changes:

- `Stage` now carries:
  - `stage_root: Path | str | None`
  - `clean_dirs: bool = False`
- `stage_root` overrides default path composition and is treated as the exact
  stage base path.

Code example:

```python
# src/caravel/pipeline.py
@dataclass
class Stage:
    name: str
    entries: list[Step | Branch | Callable[..., Any]]
    stage_root: Path | str | None = None
    clean_dirs: bool = False
```

### 3) Selective-rerun path consistency

File:

- `src/caravel/runner.py`

Changes:

- Introduced centralized stage-base resolution for both:
  - normal execution writes
  - prior-output lookup during selective stage/step execution
- This prevents mismatches between default stage paths and overridden
  `stage_root` paths when reloading prior outputs.

Code example:

```python
def _resolve_stage_base(stage_index: int) -> RunPath:
    stage_decl = pipeline.stages[stage_index]
    if stage_decl.stage_root is not None:
        return _coerce_run_path(stage_decl.stage_root)
    return _join_run_path(
        resolved_run_root,
        pipeline.name,
        format_stage_dir(stage_index + 1, stage_decl.name),
    )

# selective prior-output lookup uses the same resolver
prev_stage_dir = _resolve_stage_base(stage_index - 1)
prev_dir = _join_run_path(
    prev_stage_dir,
    format_step_dir(len(prev_stage.entries), prev_step_name),
)
```

### 4) Cleanup semantics hardened for remote roots/containers

File:

- `src/caravel/runner.py`

Cleanup behavior now uses explicit boundary semantics:

- If `stage_root` is set, cleanup boundary is `stage_root`.
- If `stage_root` is not set, cleanup boundary is `run_root`.

Important implementation details:

- `clean_dirs=True` removes descendants under the cleanup boundary.
- The boundary root object is preserved (do not delete the root itself).
  - This avoids deleting the Azure container root object/path while still
    clearing its contents.
- For selective reruns:
  - `clean_dirs=True` with non-first targeted step remains fail-fast with
    explicit `ValueError`.
- Execution order was adjusted so stage seed loading happens before cleanup,
  preventing later-stage cleanup from removing prior outputs before they are
  read.

Code example:

```python
def _resolve_stage_clean_root(stage_index: int) -> RunPath:
    stage_decl = pipeline.stages[stage_index]
    if stage_decl.stage_root is not None:
        return _coerce_run_path(stage_decl.stage_root)
    return resolved_run_root

def _clean_stage_base_if_needed(clean_root: RunPath, clean_dirs: bool) -> None:
    if not clean_dirs:
        return
    fs, root = resolve_fs(clean_root)
    if not fs.exists(root):
        fs.makedirs(root, exist_ok=True)
        return
    if not is_dir(fs, root):
        fs.rm(root)
        fs.makedirs(root, exist_ok=True)
        return
    root_prefix = root.rstrip("/")
    for child in fs.ls(root, detail=False):
        child_path = str(child["name"]) if isinstance(child, dict) else str(child)
        if root_prefix and not child_path.startswith(f"{root_prefix}/") and child_path != root:
            child_path = join_path(root, child_path)
        if child_path == root:
            continue
        fs.rm(child_path, recursive=True)
    fs.makedirs(root, exist_ok=True)

# per-stage order in run loop
partitions = _load_stage_seed(stage_index, stage.name)
clean_root = _resolve_stage_clean_root(stage_index)
_clean_stage_base_if_needed(clean_root, stage.clean_dirs)
```

Operational implication:

- Using `clean_dirs=True` on a later stage with default boundary (`run_root`)
  will clear earlier stage artifacts under that same run root after they are
  loaded for that stage.

### 5) Example/test alignment fixes after package migration

Files:

- `examples/fsspec_minimal/pipeline.py`
- `tests/examples/test_fsspec_pipeline.py`

Changes:

- Removed hardcoded Azure `stage_root` defaults from `fsspec_minimal`.
- Stage roots are now optional/configurable (`bronze_stage_root`,
  `silver_stage_root`) and can be sourced from env:
  - `CARAVEL_BRONZE_STAGE_ROOT`
  - `CARAVEL_SILVER_STAGE_ROOT`
- Test fixture reference updated from `examples/fsspec/...` to
  `examples/fsspec_minimal/...`.
- Test env cleanup now clears stage-root env vars to prevent accidental
  cross-environment leakage.

Reason:

- Local/CI tests should not require Azure credentials or `abfs://` writes unless
  explicitly configured.

Code example:

```python
# examples/fsspec_minimal/pipeline.py
resolved_bronze_stage_root = _as_path_or_url(
    bronze_stage_root
    if bronze_stage_root is not None
    else os.getenv("CARAVEL_BRONZE_STAGE_ROOT")
)
resolved_silver_stage_root = _as_path_or_url(
    silver_stage_root
    if silver_stage_root is not None
    else os.getenv("CARAVEL_SILVER_STAGE_ROOT")
)

Stage(name=BRONZE_STAGE_NAME, stage_root=resolved_bronze_stage_root, entries=[bronze_collect])
Stage(name=SILVER_STAGE_NAME, stage_root=resolved_silver_stage_root, entries=[silver_transform, silver_finalize])
```

### 6) Verification status after addendum changes

Command:

- `pytest -q`

Result:

- `162 passed`
