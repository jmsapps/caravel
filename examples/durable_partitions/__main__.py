"""Command line for the durability demo.

python -m examples.durable_partitions --job-id demo-1 --fail-at publish
python -m examples.durable_partitions --job-id demo-1
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from ..support import example_output
from .pipeline import run_demo

DEFAULT_OUTPUT = example_output(__file__)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="examples.durable_partitions",
        description=(
            "Durability demo. Run it with --fail-at to stop part way, then run "
            "it again with the same --job-id to watch committed steps come back."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Where generated output goes. Default: {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help=(
            "Identity this run resumes under. Supplying it makes the run "
            "durable; omitting it runs every step in memory."
        ),
    )
    parser.add_argument(
        "--commit-scope",
        default="step",
        choices=("pipeline", "stage", "step"),
        help=(
            "When transactions commit. 'step' keeps each completed step; "
            "'pipeline' keeps nothing until the whole run succeeds. Default: step."
        ),
    )
    parser.add_argument(
        "--fail-at",
        default=None,
        choices=("enrich", "publish", "summarize"),
        help="Raise an unrecoverable error in this step, to interrupt the run.",
    )
    parser.add_argument(
        "--flaky-attempts",
        type=int,
        default=0,
        help=(
            "Fail this many publish attempts with a transient error before "
            "succeeding. Publish declares retries, so it recovers in memory."
        ),
    )
    args = parser.parse_args(argv)

    try:
        result = run_demo(
            output=args.output,
            job_id=args.job_id,
            commit_scope=args.commit_scope,
            fail_at=args.fail_at,
            flaky_attempts=args.flaky_attempts,
        )
    except Exception as error:
        print(f"\nrun failed: {error}")
        if not args.job_id:
            print("no job id was supplied, so this run kept nothing")
        elif args.commit_scope == "pipeline":
            print(
                f"commit_scope=pipeline commits once, on success, so job_id="
                f"{args.job_id} kept nothing from this attempt; rerun without "
                "--fail-at and every step runs again"
            )
        else:
            print(
                f"steps committed before the failure are kept under job_id="
                f"{args.job_id}; rerun the same command without --fail-at to "
                "restore them and continue"
            )
        return 1

    print(
        f"\npipeline={result.pipeline_name} job_id={result.job_id} "
        f"executed={result.steps_executed} restored={result.steps_restored}"
    )
    print(f"output={result.output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
