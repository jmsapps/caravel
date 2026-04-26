# Partitioned Example

## Purpose

This example demonstrates partitioned dataset behavior with the canonical runner
layout:

- `PartitionedTextDataset` output in bronze (`.html` partitions).
- `PartitionedJSONDataset` output in silver with flat partition keys.
- Path-style key nesting in gold (`en/<id>`, `fr/<id>`).

Pipeline identifiers:

- Pipeline: `partitioned_example`
- Stages: `bronze`, `silver`, `gold`
- Steps: `bronze_render_html`, `silver_extract_structured`,
  `gold_partition_by_language`

## Fixtures

- Input fixture folder: `examples/partitioned/data/input_partitions/`
- Loader payload shape: `dict[str, dict]` from `PartitionedJSONDataset`
- Included fixture keys: `alpha`, `beta`, `gamma`

## CLI Commands

Run these commands from the repo root with Bash or another POSIX-compatible
shell.

### Run The Full Pipeline

```bash
python3 -m examples.partitioned --run-root data/partitioned_example/smoke_run
```

### Run With A Fresh Output Directory

```bash
rm -rf data/partitioned_example/smoke_run
python3 -m examples.partitioned --run-root data/partitioned_example/smoke_run
```

### Run Only One Stage

```bash
python3 -m examples.partitioned --run-root data/partitioned_example/smoke_run --stage gold
```

Selective stage execution requires the prior stage output to already exist under
the same `--run-root`.

### Run Only One Step Within A Stage

```bash
python3 -m examples.partitioned --run-root data/partitioned_example/smoke_run --stage gold --step gold_partition_by_language
```

### Pass A Runtime Parameter

```bash
python3 -m examples.partitioned --run-root data/partitioned_example/smoke_run --param mode=smoke
```

This example accepts runtime parameters through the shared CLI, but the current
steps do not use `mode`.

### Emit A Mermaid Graph

```bash
python3 -m examples.partitioned --mermaid data/partitioned_example/graph.mmd
```

### See All CLI Flags

```bash
python3 -m examples.partitioned --help
```

## Where To Inspect Outputs

After running, outputs are written under your `--run-root`.

- Bronze output folder:
  `data/partitioned_example/smoke_run/_001_bronze/_001_bronze_render_html/`
- Silver output folder:
  `data/partitioned_example/smoke_run/_002_silver/_001_silver_extract_structured/`
- Gold output folder:
  `data/partitioned_example/smoke_run/_003_gold/_001_gold_partition_by_language/`

Example nested gold files:

- `data/partitioned_example/smoke_run/_003_gold/_001_gold_partition_by_language/en/alpha.json`
- `data/partitioned_example/smoke_run/_003_gold/_001_gold_partition_by_language/fr/beta.json`

## Smoke Test Flow

1. Run the full pipeline using the command above.
2. Verify bronze contains `.html` partition files for `alpha`, `beta`, and
   `gamma`.
3. Verify silver contains flat `.json` partition files for the same keys.
4. Verify gold contains nested language output paths (`en/...`, `fr/...`).
5. Open one silver file and one gold file to confirm JSON is parseable.

## Expected Behaviors

- Stages execute in order: `bronze`, `silver`, `gold`.
- Bronze writes text partitions as `.html` files.
- Silver writes JSON partitions using flat keys.
- Gold writes JSON partitions using path-style keys that produce nested folders.
- Re-running with the same fixtures and explicit `--run-root` is
  value-deterministic.

## Failure Signals

- Missing fixture directory or file raises `FileNotFoundError` on loader dataset
  read.
- Missing expected stage/step folders indicates runner layout regression.
- Missing `en/` or `fr/` nested gold files indicates path-style key regression.
- Invalid JSON in silver or gold outputs indicates dataset serialization
  regression.
