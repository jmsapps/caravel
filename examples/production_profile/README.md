# Production Profile Qualification

This example composes the production observer explicitly around the
`fsspec_minimal` pipeline. It is a qualification fixture, not a claim that every
workload or fsspec backend is production-qualified.

Published output and evidence metadata use separate roots and may each be a
local path or an fsspec URL. The authoritative progress store is different: it
requires a local filesystem that survives invocations, so `--state` is always
local.

```bash
CARAVEL_METADATA_ROOT=data/qualification/metadata \
python3 -m examples.production_profile \
  --output data/qualification/output \
  --state data/qualification/state \
  --job-id qualification-1
```

Re-invoking the same job id restores committed work instead of recomputing it,
which is the durable claim worth qualifying:

```bash
CARAVEL_METADATA_ROOT=data/qualification/metadata \
python3 -m examples.production_profile \
  --output data/qualification/output \
  --state data/qualification/state \
  --job-id qualification-1    # reports batches=0
```

Forcing recomputation is an API call rather than a CLI selector:
`DurableStorage.invalidate(pipeline, job_id=..., step=...)`, documented in the
task record's `selective-recomputation.md`.

For Azure, install `.[azure,examples]`, set the credential variables documented
by `examples/fsspec_minimal`, and use a fresh isolated prefix for every
qualification attempt:

```bash
QUALIFICATION_ID="$(date -u +%Y%m%dT%H%M%SZ)"
CARAVEL_INPUT_URL=abfs://caravel/input/input_partitions.json \
CARAVEL_METADATA_ROOT="abfs://caravel/qualification/${QUALIFICATION_ID}/metadata" \
python3 -m examples.production_profile \
  --output "abfs://caravel/qualification/${QUALIFICATION_ID}/output" \
  --state "/var/tmp/caravel-state/${QUALIFICATION_ID}" \
  --job-id "${QUALIFICATION_ID}"
```

Do not reuse the example's fixed `smoke_run` path as release evidence. A unique
prefix prevents a previous run from making a failed qualification look valid.

## Release-gated Azure contracts

`.github/workflows/azure-qualification.yml` is the reference qualification
scheduler. GitHub Actions serializes runs through one concurrency group and
terminates the whole job after 15 minutes. The workflow accepts an account key,
SAS token, or complete service-principal credential set from repository secrets;
credentials are never written to qualification artifacts.

In addition to the example run and its resume, the workflow pins
`CARAVEL_QUALIFICATION_ARTIFACTS_ROOT` to a unique Azure prefix and runs
`tests/qualification/test_remote_artifacts.py` against the real service rather
than an emulator. That suite verifies, on committed artifacts:

- a job commits and reads artifacts on real object storage;
- committed artifacts survive reopening the store;
- resuming reads committed remote output instead of recomputing it;
- an unreferenced remote attempt stays invisible; and
- recomputation supersedes remote output without deleting it.

Capabilities that the durable store absorbed during C-8 — checkpoint evidence,
ownership inventory, and leases — are no longer plugin behavior and are covered
by the store's own suites.

The workflow publishes JUnit timings and installed dependency versions as a
sanitized GitHub artifact. Azure objects are intentionally retained beneath the
unique qualification prefix for inspection and must be covered by an explicit
storage lifecycle/retention policy.

This suite qualifies the tested Azure Blob configuration only. It does not
qualify every fsspec backend, establish production workload scale thresholds,
compare business outputs with a current system, or replace independent runbook
and owner approval.
