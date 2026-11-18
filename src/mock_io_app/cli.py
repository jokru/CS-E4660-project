import argparse
import os
import random
import sys
import tempfile
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import cache
from pathlib import Path

from dagster import (
    DagsterEvent,
    DagsterInstance,
    JobDefinition,
    build_reconstructable_job,
    execute_job,
)
from dagster._core.execution.build_resources import wrap_resources_for_execution

from mock_io_app.job_factory import ITERATION_TAG, SEED_TAG, build_job, default_resources
from mock_io_app.scenario import ExpandedJob, expand_jobs, is_valid_name, leaf_jobs, load_scenario
from mock_io_app.tracing import as_service, get_tracer, setup_tracing, trace_tags


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run scenario jobs in-process (no webserver).")
    parser.add_argument("--scenario", type=Path, required=True, help="path to scenario YAML")
    parser.add_argument(
        "--job",
        action="append",
        default=[],
        help="expanded job name (with or without '<scenario>__' prefix); repeatable",
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--latency-scale", type=float, default=1.0)
    parser.add_argument(
        "--executor",
        choices=["scenario", "in_process", "multiprocess"],
        default="scenario",
        help="Dagster executor; 'scenario' (default) uses each job's `executor`. With "
        "multiprocess, independent jobs inside a run execute in parallel child processes "
        "(at most the CPU count, or DAGSTER_MULTIPROCESS_EXECUTOR_MAX_CONCURRENT)",
    )
    load = parser.add_argument_group(
        "load mode",
        "Instead of running the jobs back to back, start --runs runs of randomly chosen jobs "
        "at Poisson arrival times (--rate per second), up to --workers at once.",
    )
    load.add_argument("--rate", type=float, help="mean run arrivals per second; enables load mode")
    load.add_argument("--runs", type=int, default=50, help="total runs in load mode")
    load.add_argument("--workers", type=int, default=4, help="max concurrent runs in load mode")
    parser.add_argument(
        "--seed",
        type=int,
        help="run seed: overrides the scenario's defaults.seed for call outcomes and, in load "
        "mode, also seeds arrivals and job choice (default 0)",
    )
    return parser.parse_args(argv)


@cache
def reconstruct_job(
    scenario_path: str, job_name: str, executor: str, latency_scale: float
) -> JobDefinition:
    """Builds a CLI job; also called in the multiprocess executor's child processes."""
    setup_tracing("mock-io-cli")
    path = Path(scenario_path)
    by_name = {j.name: j for j in expand_jobs(path.stem, load_scenario(path))}
    job_def = build_job(by_name[job_name], by_name, executor)
    resources = wrap_resources_for_execution(default_resources(latency_scale))
    return job_def.with_top_level_resources(resources)


def _run_once(
    expanded: ExpandedJob,
    args: argparse.Namespace,
    iteration: int,
    instance: DagsterInstance,
) -> tuple[int, str, bool, float, str, str]:
    executor = expanded.spec.executor if args.executor == "scenario" else args.executor
    job = build_reconstructable_job(
        "mock_io_app.cli",
        "reconstruct_job",
        (str(args.scenario.resolve()), expanded.name, executor, args.latency_scale),
    )
    attributes = {"dagster.job": expanded.name, "mock_io.iteration": iteration}
    with (
        as_service(expanded.scenario_service_name),
        get_tracer().start_as_current_span(f"run.{expanded.name}", attributes=attributes),
    ):
        started = time.perf_counter()
        tags = {**trace_tags(), ITERATION_TAG: str(iteration), **_seed_tags(args.seed)}
        with execute_job(job, instance, tags=tags, raise_on_error=False) as result:
            success, run_id = result.success, result.run_id
            reason = "" if success else _failure_reason(result.all_events)
        elapsed = time.perf_counter() - started
    return iteration, expanded.name, success, elapsed, run_id, reason


def _seed_tags(seed: int | None) -> dict[str, str]:
    return {} if seed is None else {SEED_TAG: str(seed)}


def _failure_reason(events: list[DagsterEvent]) -> str:
    """First error message of a failed run (a step failure, or e.g. the executor not starting)."""
    for event in events:
        error = getattr(event.event_specific_data, "error", None)
        if error is not None:
            return f"{event.step_key or event.event_type_value}: {error.message.strip()}"
    return next((e.message or "" for e in events if e.is_failure), "")


def _print_result(result: tuple[int, str, bool, float, str, str]) -> bool:
    iteration, name, success, elapsed, run_id, reason = result
    status = "SUCCESS" if success else "FAILURE"
    print(f"{iteration}\t{name}\t{status}\t{elapsed:.3f}s\t{run_id}")
    if reason:
        print(f"  {reason}", file=sys.stderr)
    return success


@contextmanager
def _instance(persistent: bool) -> Iterator[DagsterInstance]:
    """DAGSTER_HOME if set (runs show up in `dagster dev`). Otherwise a throwaway instance; the
    multiprocess executor needs one on disk so child processes can share it."""
    if os.environ.get("DAGSTER_HOME"):
        with DagsterInstance.get() as instance:
            yield instance
    elif persistent:
        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.local_temp(tmp) as instance:
            yield instance
    else:
        with DagsterInstance.ephemeral() as instance:
            yield instance


def _run_load(jobs: list[ExpandedJob], args: argparse.Namespace, persistent: bool) -> int:
    """Poisson arrivals of randomly chosen jobs.

    Every len(jobs) runs form one iteration, so --runs N * len(jobs) covers the same anomaly
    windows as --repeat N.
    """
    rng = random.Random(args.seed or 0)
    start = time.monotonic()
    arrival = 0.0

    def run(n: int, expanded: ExpandedJob):
        with _instance(persistent) as instance:
            return _run_once(expanded, args, n, instance)

    with ThreadPoolExecutor(args.workers) as pool:
        futures = []
        for n in range(args.runs):
            arrival += rng.expovariate(args.rate)
            time.sleep(max(0.0, start + arrival - time.monotonic()))
            futures.append(pool.submit(run, n // len(jobs), rng.choice(jobs)))
        return sum(not _print_result(f.result()) for f in futures)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    scenario_name = args.scenario.stem
    if not is_valid_name(scenario_name):
        print(f"invalid scenario file name: {scenario_name!r}", file=sys.stderr)
        return 2

    all_jobs = expand_jobs(scenario_name, load_scenario(args.scenario))
    # Without --job, run only leaf jobs: each already includes its upstream jobs' ops.
    jobs = leaf_jobs(all_jobs)
    if args.job:
        wanted = {
            n if n.startswith(f"{scenario_name}__") else f"{scenario_name}__{n}" for n in args.job
        }
        jobs = [j for j in all_jobs if j.name in wanted]
        missing = wanted - {j.name for j in jobs}
        if missing:
            print(f"unknown jobs: {sorted(missing)}", file=sys.stderr)
            return 2

    setup_tracing("mock-io-cli")
    persistent = args.executor == "multiprocess" or (
        args.executor == "scenario" and any(j.spec.executor == "multiprocess" for j in jobs)
    )
    if args.rate:
        failures = _run_load(jobs, args, persistent)
    else:
        failures = 0
        with _instance(persistent) as instance:
            for iteration in range(args.repeat):
                for expanded in jobs:
                    result = _run_once(expanded, args, iteration, instance)
                    failures += not _print_result(result)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
