from dagster import (
    In,
    JobDefinition,
    Nothing,
    OpDefinition,
    OpExecutionContext,
    RetryPolicy,
    in_process_executor,
    job,
    multiprocess_executor,
    op,
)

from mock_io_app.executor import IOResources, make_rng, run_call_group
from mock_io_app.resources import FakeDatabase, FakeGCS, FakeHttpClient
from mock_io_app.scenario import ExpandedJob, OpSpec, closure
from mock_io_app.tracing import context_from_tags, get_tracer

SCENARIO_TAG = "mock_io/scenario"

_EXECUTORS = {"in_process": in_process_executor, "multiprocess": multiprocess_executor}


def default_resources(latency_scale: float = 1.0) -> dict[str, object]:
    return {
        "db": FakeDatabase(latency_scale=latency_scale),
        "gcs": FakeGCS(latency_scale=latency_scale),
        "http": FakeHttpClient(latency_scale=latency_scale),
    }


def build_op(job_name: str, owner: ExpandedJob, spec: OpSpec, n_after: int) -> OpDefinition:
    async def compute(
        context: OpExecutionContext, db: FakeDatabase, gcs: FakeGCS, http: FakeHttpClient
    ) -> int:
        resources = IOResources(db=db, gcs=gcs, http=http)
        attributes = {
            "dagster.run_id": context.run_id,
            "dagster.job": job_name,
            "dagster.op": spec.name,
            "dagster.retry_number": context.retry_number,
            "mock_io.scenario": owner.scenario,
            "mock_io.source_job": owner.name,
        }
        with get_tracer().start_as_current_span(
            f"op.{spec.name}", context=context_from_tags(context.run.tags), attributes=attributes
        ):
            total = 0
            for index, group in enumerate(spec.call_groups):
                rng = make_rng(
                    owner.defaults.seed, owner.name, spec.name, index, context.retry_number
                )
                total += await run_call_group(
                    group,
                    owner.defaults,
                    resources,
                    rng,
                    key_prefix=f"{owner.name}/{spec.name}/{index}",
                )
        context.log.info("op %s completed %d calls", spec.name, total)
        return total

    # Definition names must be unique per code location; the node is aliased in the graph.
    return op(
        name=f"{job_name}__{owner.name}__{spec.name}",
        ins={f"after_{i}": In(Nothing) for i in range(n_after)},
        retry_policy=RetryPolicy(max_retries=spec.retries) if spec.retries else None,
    )(compute)


def build_job(expanded: ExpandedJob, all_jobs: dict[str, ExpandedJob]) -> JobDefinition:
    members = closure(expanded, all_jobs)
    ops = {
        owner.name: [
            (spec, build_op(expanded.name, owner, spec, len(owner.upstream) if i == 0 else 1))
            for i, spec in enumerate(owner.spec.ops)
        ]
        for owner in members
    }

    @job(
        name=expanded.name,
        executor_def=_EXECUTORS[expanded.spec.executor],
        tags={SCENARIO_TAG: expanded.scenario},
    )
    def _job() -> None:
        last_output = {}
        for owner in members:
            previous = [last_output[name] for name in owner.upstream]
            for spec, op_def in ops[owner.name]:
                node = op_def.alias(f"{owner.name}__{spec.name}")
                output = node(**{f"after_{i}": p for i, p in enumerate(previous)})
                previous = [output]
            last_output[owner.name] = previous[0]

    return _job
