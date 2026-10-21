import argparse
import sys
import time
from pathlib import Path

from dagster import DagsterInstance

from mock_io_app.job_factory import build_job, default_resources
from mock_io_app.scenario import expand_jobs, is_valid_name, leaf_jobs, load_scenario
from mock_io_app.tracing import get_tracer, setup_tracing, trace_tags


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
    return parser.parse_args(argv)


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
    tracer = get_tracer()
    resources = default_resources(args.latency_scale)
    definitions = [(expanded, build_job(expanded, by_name)) for expanded in jobs]
    failures = 0

    with DagsterInstance.ephemeral() as instance:
        for iteration in range(args.repeat):
            for expanded, job_def in definitions:
                attributes = {"dagster.job": expanded.name, "mock_io.iteration": iteration}
                with tracer.start_as_current_span(f"job.{expanded.name}", attributes=attributes):
                    started = time.perf_counter()
                    result = job_def.execute_in_process(
                        resources=resources,
                        instance=instance,
                        tags=trace_tags(),
                        raise_on_error=False,
                    )
                    elapsed = time.perf_counter() - started
                outcome = "SUCCESS" if result.success else "FAILURE"
                failures += not result.success
                print(f"{iteration}\t{expanded.name}\t{outcome}\t{elapsed:.3f}s\t{result.run_id}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
