# Production Profile Qualification

This example composes the four first-party production plugins explicitly around
the `fsspec_minimal` pipeline. It is a qualification fixture, not a claim that
every workload or fsspec backend is production-qualified.

Use separate output and metadata roots. Both may be local paths or fsspec URLs:

```bash
CARAVEL_METADATA_ROOT=data/qualification/metadata \
python3 -m examples.production_profile \
  --run-root data/qualification/output
```

After the full run, exercise checkpoint-backed selective execution against the
same roots:

```bash
CARAVEL_METADATA_ROOT=data/qualification/metadata \
python3 -m examples.production_profile \
  --run-root data/qualification/output \
  --stage silver
```

For Azure, install `.[azure,examples]`, set the credential variables documented
by `examples/fsspec_minimal`, and use a fresh isolated prefix for every
qualification attempt:

```bash
QUALIFICATION_ID="$(date -u +%Y%m%dT%H%M%SZ)"
CARAVEL_INPUT_URL=abfs://caravel/input/input_partitions.json \
CARAVEL_METADATA_ROOT="abfs://caravel/qualification/${QUALIFICATION_ID}/metadata" \
python3 -m examples.production_profile \
  --run-root "abfs://caravel/qualification/${QUALIFICATION_ID}/output"
```

Do not reuse the example's fixed `smoke_run` path as release evidence. A unique
prefix prevents a previous run from making a failed qualification look valid.
