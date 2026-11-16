import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace

from mock_io_app.latency import sample_seconds
from mock_io_app.resources import (
    CallParams,
    FakeDatabase,
    FakeGCS,
    FakeHttpClient,
    FakeServiceClient,
)
from mock_io_app.scenario import AnomalySpec, CallGroupSpec, Defaults, ServiceSpec
from mock_io_app.tracing import get_tracer

Call = Callable[[CallParams], Awaitable[object]]


@dataclass(frozen=True)
class IOResources:
    db: FakeDatabase
    gcs: FakeGCS
    http: FakeHttpClient
    services: FakeServiceClient = field(default_factory=FakeServiceClient)


def make_rng(seed: int | None, *parts: object) -> random.Random:
    if seed is None:
        return random.Random()
    return random.Random(":".join(str(p) for p in (seed, *parts)))


def build_call_params(
    group: CallGroupSpec,
    defaults: Defaults,
    rng: random.Random,
    iteration: int = 0,
    service_anomalies: Sequence[AnomalySpec] = (),
) -> list[CallParams]:
    latency = group.latency or defaults.latency
    failure_rate = defaults.failure_rate if group.failure_rate is None else group.failure_rate
    anomalies = [a for a in (*group.anomalies, *service_anomalies) if a.active(iteration)]
    # Drawn only when anomalies exist, so scenarios without them keep their outcomes. The
    # iteration is mixed in so probabilistic anomalies hit different calls on each iteration.
    anomaly_rng = random.Random(f"{rng.getrandbits(64)}:{iteration}") if anomalies else rng
    # Pre-sampled so outcomes don't depend on gather scheduling order.
    params = []
    for i in range(group.count):
        call = CallParams(
            latency_s=sample_seconds(latency, rng),
            fail=rng.random() < failure_rate,
            payload_bytes=group.payload_bytes,
            index=i,
        )
        for anomaly in anomalies:
            if anomaly_rng.random() < anomaly.probability:
                call = _apply_anomaly(call, anomaly)
        params.append(call)
    return params


def _apply_anomaly(call: CallParams, anomaly: AnomalySpec) -> CallParams:
    return replace(
        call,
        latency_s=call.latency_s * anomaly.latency_multiplier + anomaly.latency_add_ms / 1000,
        fail=call.fail or anomaly.fail,
        payload_bytes=(
            call.payload_bytes if anomaly.payload_bytes is None else anomaly.payload_bytes
        ),
        anomalies=(*call.anomalies, anomaly.label),
    )


def resolve_call(
    group: CallGroupSpec,
    res: IOResources,
    key_prefix: str,
    defaults: Defaults,
    services: Mapping[str, ServiceSpec],
    handler_seed: int = 0,
    iteration: int = 0,
) -> Call:
    match group.io, group.action:
        case "db", "query":
            return res.db.query
        case "db", "insert":
            return res.db.insert
        case "gcs", "upload":
            return lambda p: res.gcs.upload(f"{key_prefix}/{p.index}", p)
        case "gcs", "download":
            return lambda p: res.gcs.download(f"{key_prefix}/{p.index}", p)
        case "gcs", "list":
            return lambda p: res.gcs.list_blobs(key_prefix, p)
        case "http", "get":
            return lambda p: res.http.get(f"/{key_prefix}/{p.index}", p)
        case "http", "post":
            return lambda p: res.http.post(f"/{key_prefix}", p)
        case "service", "call":
            name = group.service or ""
            spec = services[name]
            if spec.kind == "third_party":
                return lambda p: res.services.call(name, p)

            def first_party(p: CallParams) -> Awaitable[object]:
                async def handler() -> None:
                    for i, inner in enumerate(spec.call_groups):
                        await run_call_group(
                            inner,
                            defaults,
                            res,
                            random.Random(f"{handler_seed}:{p.index}:{i}"),
                            f"{key_prefix}/{p.index}/{name}/{i}",
                            services,
                            iteration,
                        )

                return res.services.call(name, p, handler)

            return first_party
    raise ValueError(f"unsupported call {group.io}.{group.action}")


async def run_call_group(
    group: CallGroupSpec,
    defaults: Defaults,
    resources: IOResources,
    rng: random.Random,
    key_prefix: str,
    services: Mapping[str, ServiceSpec] | None = None,
    iteration: int = 0,
) -> int:
    services = services or {}
    service_spec = services.get(group.service or "")
    params = build_call_params(
        group, defaults, rng, iteration, service_spec.anomalies if service_spec else ()
    )
    # Derived from the group's rng so a service's inner outcomes change across op retries.
    handler_seed = rng.getrandbits(64) if group.io == "service" else 0
    call = resolve_call(group, resources, key_prefix, defaults, services, handler_seed, iteration)
    attributes = {
        "mock_io.type": group.io,
        "mock_io.action": group.action,
        "mock_io.mode": group.mode,
        "mock_io.count": group.count,
        "mock_io.max_concurrency": group.max_concurrency or 0,
    }
    if group.service:
        attributes["mock_io.service"] = group.service
    with get_tracer().start_as_current_span(
        f"call_group.{group.io}.{group.action}", attributes=attributes
    ):
        if group.mode == "sequential":
            for p in params:
                await call(p)
        else:
            await _gather(call, params, group.max_concurrency)
    return group.count


async def _gather(call: Call, params: list[CallParams], max_concurrency: int | None) -> None:
    semaphore = asyncio.Semaphore(max_concurrency or len(params))

    async def guarded(p: CallParams) -> None:
        async with semaphore:
            await call(p)

    try:
        async with asyncio.TaskGroup() as tg:
            for p in params:
                tg.create_task(guarded(p))
    except ExceptionGroup as eg:
        raise eg.exceptions[0] from eg
