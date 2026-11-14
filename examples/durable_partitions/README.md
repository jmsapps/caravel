# Durability demo

Three steps that sleep, log, and commit. Interrupt the run, then run it again
with the same job id and watch the finished steps come back instead of being
recomputed.

```console
$ python -m examples.durable_partitions --help
```

| Flag | Meaning |
| --- | --- |
| `--output PATH` | Where generated output goes. Default `examples/durable_partitions/data/output`. |
| `--job-id ID` | The identity this run resumes under. Supplying it makes the run durable. |
| `--commit-scope SCOPE` | `pipeline`, `stage`, or `step`. Default `step`. |
| `--fail-at STEP` | Raise an unrecoverable error in `enrich`, `publish`, or `summarize`. |
| `--flaky-attempts N` | Fail N publish attempts with a transient error first. |

Each step sleeps 0.25s per document over 8 documents, so roughly two seconds of
work per step is either kept or repeated — long enough to see.

## 1. Interrupt a run, then continue it

```console
$ python -m examples.durable_partitions --job-id demo-1 --fail-at publish
[15:53:37] enrich     ▶ processing 8 documents
[15:53:39] enrich     ✔ enriched 8 documents
[15:53:39] publish    ▶ processing 8 documents
[15:53:41] publish    ✘ injected failure

run failed: Injected failure in 'publish'
steps committed before the failure are kept under job_id=demo-1; rerun the same
command without --fail-at to restore them and continue
```

`enrich` finished and committed. `publish` failed, so it wrote no snapshot.
Run it again with the same job id:

```console
$ python -m examples.durable_partitions --job-id demo-1
[15:53:48] enrich     ↺ restored from snapshot (no work repeated)
[15:53:48] publish    ▶ processing 8 documents
[15:53:50] publish    ✔ published 8 documents
[15:53:50] summarize  ▶ processing 8 documents
[15:53:52] summarize  ✔ 8 documents, 108 words

pipeline=durable_demo job_id=demo-1 executed=2 restored=1
```

`enrich` never slept: its committed result was read back from the database.
`restored=1` counts it.

The `↺` lines come from `StepLogger`, an observer in `pipeline.py`. A step that
completes without starting was restored rather than executed.

## 2. Watch a retry recover a transient failure

`publish` declares `RetryOptions`, so a transient error is retried in memory
before anything is committed:

```console
$ python -m examples.durable_partitions --job-id demo-2 --flaky-attempts 2
[15:54:11] publish    ▶ processing 8 documents
[15:54:13] publish    ↻ transient failure on attempt 1; retrying
[15:54:13] publish    ▶ processing 8 documents
[15:54:15] publish    ↻ transient failure on attempt 2; retrying
[15:54:15] publish    ▶ processing 8 documents
[15:54:17] publish    ✔ published 8 documents
```

Only the accepted result is stored. The failed attempts left no snapshot and no
completion marker behind.

## 3. See what commit scope changes

`--commit-scope` selects when transactions commit. Fail at the last step under
each scope and compare what survives:

```console
$ python -m examples.durable_partitions --job-id demo-3 --commit-scope pipeline --fail-at summarize
...
commit_scope=pipeline commits once, on success, so job_id=demo-3 kept nothing
from this attempt; rerun without --fail-at and every step runs again

$ python -m examples.durable_partitions --job-id demo-3 --commit-scope pipeline
pipeline=durable_demo job_id=demo-3 executed=3 restored=0
```

Under `step` scope the same interruption keeps both finished steps:

```console
$ python -m examples.durable_partitions --job-id demo-4 --commit-scope step --fail-at summarize
$ python -m examples.durable_partitions --job-id demo-4 --commit-scope step
pipeline=durable_demo job_id=demo-4 executed=1 restored=2
```

Every successful boundary writes its rows under all three scopes. What differs
is when those writes become durable.

## 4. Run without durability

Omit `--job-id` and no recovery database is created. The same processors run
with the same retry controls, and every step runs again next time.

```console
$ python -m examples.durable_partitions
pipeline=durable_demo job_id=None executed=3 restored=0
```

## 5. Delete the output and get it back

SQLite is the recovery authority, so generated output is reproducible:

```console
$ rm -rf examples/durable_partitions/data/output
$ python -m examples.durable_partitions --job-id demo-1
[..] enrich     ↺ restored from snapshot (no work repeated)
[..] publish    ↺ restored from snapshot (no work repeated)
[..] summarize  ↺ restored from snapshot (no work repeated)

pipeline=durable_demo job_id=demo-1 executed=0 restored=3
```

Every file is rewritten from committed snapshots without calling a processor.

## Where state lives

```text
examples/durable_partitions/data/
├── .caravel/             reserved — deleting it loses the ability to resume
│   ├── state.sqlite3     runs, stages, steps, and their snapshots
│   └── owner.sqlite3     executor ownership
└── output/
    ├── _001_prepare/_001_enrich/_001_enrich.json
    ├── _002_publish/_001_publish/_001_publish.json
    └── _003_summary/_001_summarize/_001_summarize.json
```
