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

## Release-gated Azure contracts

`.github/workflows/azure-qualification.yml` is the reference qualification
scheduler. GitHub Actions serializes runs through one concurrency group and
terminates the whole job after 15 minutes. The workflow accepts an account key,
SAS token, or complete service-principal credential set from repository secrets;
credentials are never written to qualification artifacts.

In addition to the full and selective example runs, the credential-gated suite
at `tests/qualification/test_azure_profile.py` verifies these behaviors on a
unique Azure prefix:

- checkpoint-backed selective execution and sanitized run events;
- reconstruction of checkpoint-proven empty partitioned output when Azure
  preserves no empty directory;
- interruption after checkpoint invalidation followed by safe recovery;
- malformed checkpoint evidence failing closed;
- ownership pruning of a removed output;
- live-lease refusal, stale-lease recovery, and normal lease cleanup;
- stale-partition removal during output replacement; and
- partition-key traversal rejection before an escaped object is written.

The workflow publishes JUnit timings and installed dependency versions as a
sanitized GitHub artifact. Azure objects are intentionally retained beneath the
unique qualification prefix for inspection and must be covered by an explicit
storage lifecycle/retention policy.

This suite qualifies the tested Azure Blob configuration only. It does not
qualify every fsspec backend, establish production workload scale thresholds,
compare business outputs with a current system, or replace independent runbook
and owner approval.
