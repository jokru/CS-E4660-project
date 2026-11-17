import argparse
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dagster import DagsterInstance, JobDefinition

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


def _run_once(
    expanded: ExpandedJob,
    job_def: JobDefinition,
    iteration: int,
    resources: dict[str, object],
    instance: DagsterInstance,
    seed: int | None = None,
) -> tuple[int, str, bool, float, str]:
    attributes = {"dagster.job": expanded.name, "mock_io.iteration": iteration}
    with (
        as_service(expanded.scenario_service_name),
        get_tracer().start_as_current_span(f"run.{expanded.name}", attributes=attributes),
    ):
        started = time.perf_counter()
        result = job_def.execute_in_process(
            resources=resources,
            instance=instance,
            tags={**trace_tags(), ITERATION_TAG: str(iteration), **_seed_tags(seed)},
            raise_on_error=False,
        )
        elapsed = time.perf_counter() - started
    return iteration, expanded.name, result.success, elapsed, result.run_id


def _seed_tags(seed: int | None) -> dict[str, str]:
    return {} if seed is None else {SEED_TAG: str(seed)}


def _print_result(result: tuple[int, str, bool, float, str]) -> bool:
    iteration, name, success, elapsed, run_id = result
    print(f"{iteration}\t{name}\t{'SUCCESS' if success else 'FAILURE'}\t{elapsed:.3f}s\t{run_id}")
    return success


def _instance() -> DagsterInstance:
    # With DAGSTER_HOME set, runs are recorded there and show up in `dagster dev`.
    return DagsterInstance.get() if os.environ.get("DAGSTER_HOME") else DagsterInstance.ephemeral()


def _run_load(
    definitions: list[tuple[ExpandedJob, JobDefinition]],
    resources: dict[str, object],
    args: argparse.Namespace,
) -> int:
    """Poisson arrivals of randomly chosen jobs; the run number is used as the iteration."""
    rng = random.Random(args.seed or 0)
    start = time.monotonic()
    arrival = 0.0

    def run(n: int, expanded: ExpandedJob, job_def: JobDefinition):
        with _instance() as instance:
            return _run_once(expanded, job_def, n, resources, instance, args.seed)

    with ThreadPoolExecutor(args.workers) as pool:
        futures = []
        for n in range(args.runs):
            arrival += rng.expovariate(args.rate)
            time.sleep(max(0.0, start + arrival - time.monotonic()))
            futures.append(pool.submit(run, n, *rng.choice(definitions)))
        return sum(not _print_result(f.result()) for f in futures)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    scenario_name = args.scenario.stem
    if not is_valid_name(scenario_name):
        print(f"invalid scenario file name: {scenario_name!r}", file=sys.stderr)
        return 2

    all_jobs = expand_jobs(scenario_name, load_scenario(args.scenario))
    by_name = {j.name: j for j in all_jobs}
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
    resources = default_resources(args.latency_scale)
    definitions = [(expanded, build_job(expanded, by_name)) for expanded in jobs]
    if args.rate:
        failures = _run_load(definitions, resources, args)
    else:
        failures = 0
        with _instance() as instance:
            for iteration in range(args.repeat):
                for expanded, job_def in definitions:
                    result = _run_once(expanded, job_def, iteration, resources, instance, args.seed)
                    failures += not _print_result(result)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
