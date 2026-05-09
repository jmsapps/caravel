# Production Readiness Roadmap

This project is currently best described as a deterministic pipeline runner with
branching support. Before it replaces Kedro or carries production workloads, the
remaining work should prove that it can be operated safely, recovered reliably,
and cut over with a clear rollback path.

## Current State

ST-01 through ST-14 establish the core framework:

- pipeline declarations with `Pipeline`, `Stage`, `Step`, and `Branch`
- dataset-owned load/save behavior
- deterministic stage/step output layout
- source-aware branch routing and merge behavior
- selective stage/step execution
- Mermaid graph rendering
- example pipelines and unit coverage

This is enough for a working proof of concept. It is not yet enough to call the
runner production-ready.

## ST-15 - Production Operations And Observability

Goal: make every run inspectable, alertable, and diagnosable.

Implementation tasks:

- Add run metadata files under each run root, including pipeline name, run id,
  start/end timestamps, status, parameters, selected stage/step, and code/version
  identifier when available.
- Record per-stage and per-step status, duration, input dataset description,
  output dataset description, output paths, and exception details.
- Emit structured logs with stable fields for pipeline, run id, stage, step,
  event type, status, duration, and error class.
- Define alertable failure signals for failed run, failed step, missing prior
  output, invalid selector, dataset save/load failure, and branch collision.
- Add operator-facing documentation for where outputs, logs, and metadata live.

Validation tasks:

- Unit tests for metadata creation on success and failure.
- Tests proving failed steps produce durable failure metadata.
- A sample failed run that is easy to inspect without reading raw stack traces.

Exit criteria:

- An operator can answer what ran, what failed, where outputs are, and what to
  rerun from files and logs alone.

## ST-16 - Reliability Hardening

Goal: make retries, timeouts, idempotency, and recovery explicit.

Implementation tasks:

- Add configurable retry policy for steps, including max attempts, backoff, and
  retryable exception classes.
- Add timeout policy for steps or document why timeout enforcement is delegated
  to the external scheduler.
- Define idempotency rules for step functions and dataset writes.
- Make output writes safer by writing to temporary paths and committing
  atomically where possible.
- Add explicit resume/recompute behavior for partial runs.
- Fix branch route step output path collisions by including route step indexes in
  route output directories.
- Validate declarations before execution, including duplicate stage names,
  duplicate step names in a stage, duplicate route step names, empty branch
  routes, invalid route keys, and unsupported selective execution shapes.

Validation tasks:

- Tests for retry success, retry exhaustion, and non-retryable failures.
- Tests for partial output cleanup or atomic commit behavior.
- Tests for duplicate branch route step names and route path stability.
- Tests for selective rerun behavior after partial failure.

Exit criteria:

- A failed or interrupted run has predictable recovery behavior and cannot
  silently corrupt prior successful outputs.

## ST-17 - Performance And Scale Qualification

Goal: prove the runner can handle expected production data volume.

Implementation tasks:

- Define target data volumes, partition counts, record sizes, and acceptable run
  durations.
- Add load/performance fixtures that mimic production partition shape.
- Profile dataset load/save behavior and branch fan-out/fan-in behavior.
- Identify memory pressure points caused by loading full partition maps at once.
- Decide whether streaming, chunking, or parallel route execution is needed.

Validation tasks:

- Repeatable performance test suite with documented thresholds.
- Baseline report for representative small, medium, and large runs.
- Regression guard for any agreed critical threshold.

Exit criteria:

- The runner meets production volume and duration targets with measured evidence,
  or the gaps are documented with remediation tasks.

## ST-18 - Deployment And Runtime Integration

Goal: make the framework runnable in the real production environment.

Implementation tasks:

- Decide the scheduler boundary: cron, GitHub Actions, Airflow, Dagster, Prefect,
  Azure, or another internal scheduler.
- Package the framework consistently for the runtime environment.
- Define environment promotion flow for dev, staging, and production.
- Define configuration loading for paths, parameters, secrets references, and
  environment-specific settings.
- Add runbook steps for deploy, run, inspect, rerun, rollback, and emergency stop.
- Add CI that installs dependencies and runs the full test suite.

Validation tasks:

- Dry run in the target runtime environment.
- CI must pass from a clean checkout.
- Runbook must be followed successfully by someone other than the author.

Exit criteria:

- The pipeline can be deployed, run, observed, rerun, and rolled back without
  relying on local developer state.

## ST-19 - Security And Compliance Hardening

Goal: ensure production data, credentials, and retained artifacts are handled
appropriately.

Implementation tasks:

- Verify no secrets are passed through `params`, logs, metadata, or output paths.
- Define how secrets are resolved by the runtime environment.
- Define access boundaries for source data, run outputs, logs, and metadata.
- Define retention policy for run artifacts.
- Add audit-friendly metadata for who/what triggered a run when the scheduler
  can provide it.
- Review dataset path handling for traversal and unsafe partition keys.

Validation tasks:

- Tests for secret redaction in logs and metadata.
- Tests for partition key/path safety.
- Review checklist for production storage permissions and retention.

Exit criteria:

- Production credentials and sensitive data cannot be accidentally exposed
  through normal framework logging, metadata, or path generation.

## ST-20 - Go-Live Gate

Goal: replace Kedro only after proving parity and rollback.

Implementation tasks:

- Select representative Kedro runs for parity comparison.
- Run this framework in shadow mode against the same inputs.
- Compare outputs, counts, checksums, key business metrics, and failure behavior.
- Define cutover criteria, rollback criteria, and ownership.
- Define what must be monitored during the first production window.

Validation tasks:

- Shadow-run report covering at least one normal run and one expected edge case.
- Signed-off cutover checklist.
- Tested rollback procedure.

Exit criteria:

- The replacement is accepted only after parity is demonstrated and rollback is
  operationally clear.

## Production Blockers To Resolve First

These items should be treated as blockers before any production pipeline depends
on this runner:

- Tests must run in CI from a clean environment.
- Branch route step output directories must include stable indexes to avoid
  collisions.
- Run metadata must persist success and failure status.
- Selective rerun semantics must be documented and covered by tests.
- Declaration validation must reject ambiguous or unsupported pipeline shapes.
- A shadow-run parity window against Kedro or the current implementation must
  pass before cutover.

## Recommended Scope Decision

For production use, decide explicitly between these two paths:

- Keep this as a small deterministic pipeline runner and harden ST-15 through
  ST-20.
- Keep the declaration API but compile or delegate execution to a proven
  orchestrator such as Kedro, Dagster, Prefect, or Airflow.

Avoid positioning this as a general DAG scheduler unless the project adds an
explicit graph model, dependency edges, topological sorting, cycle detection,
parallel scheduling, durable state, and a broader operational surface.
