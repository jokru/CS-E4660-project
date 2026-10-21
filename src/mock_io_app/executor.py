import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from mock_io_app.latency import sample_seconds
from mock_io_app.resources import CallParams, FakeDatabase, FakeGCS, FakeHttpClient
from mock_io_app.scenario import CallGroupSpec, Defaults
from mock_io_app.tracing import get_tracer

Call = Callable[[CallParams], Awaitable[object]]


@dataclass(frozen=True)
class IOResources:
    db: FakeDatabase
    gcs: FakeGCS
    http: FakeHttpClient


def make_rng(seed: int | None, *parts: object) -> random.Random:
    if seed is None:
        return random.Random()
    return random.Random(":".join(str(p) for p in (seed, *parts)))


def build_call_params(
    group: CallGroupSpec, defaults: Defaults, rng: random.Random
) -> list[CallParams]:
    latency = group.latency or defaults.latency
    failure_rate = defaults.failure_rate if group.failure_rate is None else group.failure_rate
    # Pre-sampled so outcomes don't depend on gather scheduling order.
    return [
        CallParams(
            latency_s=sample_seconds(latency, rng),
            fail=rng.random() < failure_rate,
            payload_bytes=group.payload_bytes,
            index=i,
        )
        for i in range(group.count)
    ]


def resolve_call(group: CallGroupSpec, res: IOResources, key_prefix: str) -> Call:
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
    raise ValueError(f"unsupported call {group.io}.{group.action}")


async def run_call_group(
    group: CallGroupSpec,
    defaults: Defaults,
    resources: IOResources,
    rng: random.Random,
    key_prefix: str,
) -> int:
    params = build_call_params(group, defaults, rng)
    call = resolve_call(group, resources, key_prefix)
    attributes = {
        "mock_io.type": group.io,
        "mock_io.action": group.action,
        "mock_io.mode": group.mode,
        "mock_io.count": group.count,
        "mock_io.max_concurrency": group.max_concurrency or 0,
    }
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
