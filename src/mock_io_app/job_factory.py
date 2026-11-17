from dagster import (
    In,
    JobDefinition,
    Nothing,
    OpDefinition,
    OpExecutionContext,
    in_process_executor,
    job,
    multiprocess_executor,
    op,
)

from mock_io_app.executor import IOResources, make_rng, run_call_group
from mock_io_app.resources import (
    FakeDatabase,
    FakeGCS,
    FakeHttpClient,
    FakeServiceClient,
    MockIOError,
)
from mock_io_app.scenario import ExpandedJob, OpSpec, closure
from mock_io_app.tracing import as_service, context_from_tags, get_tracer

SCENARIO_TAG = "mock_io/scenario"
ITERATION_TAG = "mock_io/iteration"

_EXECUTORS = {"in_process": in_process_executor, "multiprocess": multiprocess_executor}


def default_resources(latency_scale: float = 1.0) -> dict[str, object]:
    return {
        "db": FakeDatabase(latency_scale=latency_scale),
        "gcs": FakeGCS(latency_scale=latency_scale),
        "http": FakeHttpClient(latency_scale=latency_scale),
        "services": FakeServiceClient(latency_scale=latency_scale),
    }


def build_op(job_name: str, owner: ExpandedJob) -> OpDefinition:
    """One Dagster op per (expanded) job; its scenario ops run in order inside a `job.*` span."""

    async def compute(
        context: OpExecutionContext,
        db: FakeDatabase,
        gcs: FakeGCS,
        http: FakeHttpClient,
        services: FakeServiceClient,
    ) -> int:
        resources = IOResources(db=db, gcs=gcs, http=http, services=services)
        iteration = int(context.run.tags.get(ITERATION_TAG, 0))
        attributes = {
            "dagster.run_id": context.run_id,
            "dagster.job": job_name,
            "mock_io.scenario": owner.scenario,
            "mock_io.iteration": iteration,
        }
        total = 0
        with (
            as_service(owner.service_name),
            get_tracer().start_as_current_span(
                f"job.{owner.name}",
                context=context_from_tags(context.run.tags),
                attributes=attributes,
            ),
        ):
            for spec in owner.spec.ops:
                total += await _run_scenario_op(owner, spec, resources, iteration, context)
        context.log.info("job %s completed %d calls", owner.name, total)
        return total

    # Definition names must be unique per code location; the node is aliased in the graph.
    return op(
        name=f"{job_name}__{owner.name}",
        ins={f"after_{i}": In(Nothing) for i in range(len(owner.upstream))},
    )(compute)


async def _run_scenario_op(
    owner: ExpandedJob,
    spec: OpSpec,
    resources: IOResources,
    iteration: int,
    context: OpExecutionContext,
) -> int:
    for attempt in range(spec.retries + 1):
        attributes = {
            "dagster.op": spec.name,
            "dagster.retry_number": attempt,
            "mock_io.source_job": owner.name,
        }
        try:
            with get_tracer().start_as_current_span(f"op.{spec.name}", attributes=attributes):
                total = 0
                for index, group in enumerate(spec.call_groups):
                    rng = make_rng(owner.defaults.seed, owner.name, spec.name, index, attempt)
                    total += await run_call_group(
                        group,
                        owner.defaults,
                        resources,
                        rng,
                        key_prefix=f"{owner.name}/{spec.name}/{index}",
                        services=owner.services,
                        iteration=iteration,
                    )
            return total
        except MockIOError:
            if attempt == spec.retries:
                raise
            context.log.warning("op %s failed (attempt %d), retrying", spec.name, attempt)
    raise AssertionError("unreachable")


def build_job(expanded: ExpandedJob, all_jobs: dict[str, ExpandedJob]) -> JobDefinition:
    members = closure(expanded, all_jobs)
    op_defs = {owner.name: build_op(expanded.name, owner) for owner in members}

    @job(
        name=expanded.name,
        executor_def=_EXECUTORS[expanded.spec.executor],
        tags={SCENARIO_TAG: expanded.scenario},
    )
    def _job() -> None:
        outputs = {}
        for owner in members:
            node = op_defs[owner.name].alias(owner.name)
            outputs[owner.name] = node(
                **{f"after_{i}": outputs[up] for i, up in enumerate(owner.upstream)}
            )

    return _job
