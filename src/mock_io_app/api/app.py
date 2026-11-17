import logging
import os
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi import Path as PathParam
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import ValidationError

from mock_io_app.api.gateway import DagsterGateway, GatewayError
from mock_io_app.api.models import (
    MAX_RUNS_PER_REQUEST,
    LaunchedRun,
    LaunchRequest,
    LaunchResponse,
    RunStatusResponse,
    ScenarioSummary,
)
from mock_io_app.job_factory import ITERATION_TAG, SCENARIO_TAG, SEED_TAG
from mock_io_app.scenario import (
    MAX_SCENARIO_FILE_BYTES,
    NAME_PATTERN,
    SCENARIO_SUFFIX,
    ExpandedJob,
    Scenario,
    ScenarioSyntaxError,
    expand_jobs,
    leaf_jobs,
    load_scenarios,
    parse_scenario,
    scenario_dir,
    scenario_glob,
)
from mock_io_app.tracing import as_service, get_tracer, setup_tracing, trace_tags

logger = logging.getLogger(__name__)

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
JOB_NAME_PATTERN = r"^[a-z0-9_]{1,200}$"

ScenarioName = Annotated[str, PathParam(pattern=NAME_PATTERN)]
JobName = Annotated[str, PathParam(pattern=JOB_NAME_PATTERN)]
RunId = Annotated[str, PathParam(pattern=UUID_PATTERN)]


@lru_cache(maxsize=1)
def get_gateway() -> DagsterGateway:
    return DagsterGateway.from_env()


def get_scenario_dir() -> Path:
    return scenario_dir()


Gateway = Annotated[DagsterGateway, Depends(get_gateway)]
ScenarioDir = Annotated[Path, Depends(get_scenario_dir)]


def _load(directory: Path) -> dict[str, Scenario]:
    try:
        return load_scenarios(directory)
    except ValueError as exc:
        logger.error("failed to load scenarios from %s: %s", directory, exc)
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "scenario files invalid"
        ) from exc


def _summary(name: str, scenario: Scenario) -> ScenarioSummary:
    return ScenarioSummary(
        name=name,
        description=scenario.description,
        jobs=[job.name for job in expand_jobs(name, scenario)],
    )


def _launch(
    gateway: DagsterGateway, jobs: list[ExpandedJob], request: LaunchRequest
) -> LaunchResponse:
    if len(jobs) * request.repeat > MAX_RUNS_PER_REQUEST:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"request would launch more than {MAX_RUNS_PER_REQUEST} runs",
        )
    launched: list[LaunchedRun] = []
    for iteration in range(request.repeat):
        for job in jobs:
            # Same as the CLI: a `run.*` span per run under the request span, so everything this
            # request launches is part of the request's trace.
            attributes = {
                "dagster.job": job.name,
                "mock_io.scenario": job.scenario,
                "mock_io.iteration": iteration,
            }
            with (
                as_service(job.scenario_service_name),
                get_tracer().start_as_current_span(f"run.{job.name}", attributes=attributes),
            ):
                tags = {
                    **request.tags,
                    **trace_tags(),
                    SCENARIO_TAG: job.scenario,
                    ITERATION_TAG: str(iteration),
                    **({} if request.seed is None else {SEED_TAG: str(request.seed)}),
                }
                try:
                    run_id = gateway.submit(job.name, tags)
                except GatewayError as exc:
                    raise HTTPException(
                        exc.status_code,
                        {"message": exc.message, "launched": [r.model_dump() for r in launched]},
                    ) from exc
            launched.append(LaunchedRun(job=job.name, run_id=run_id))
    return LaunchResponse(runs=launched)


def _write_atomically(path: Path, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    try:
        os.replace(tmp_path, path)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


async def _read_limited_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "scenario too large")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "scenario too large")
    return bytes(body)


def create_app() -> FastAPI:
    setup_tracing("mock-io-api")
    app = FastAPI(title="mock-io-app API")

    @app.exception_handler(GatewayError)
    async def _gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse({"detail": exc.message}, status_code=exc.status_code)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/scenarios")
    def list_scenarios(directory: ScenarioDir) -> list[ScenarioSummary]:
        return [_summary(name, scenario) for name, scenario in _load(directory).items()]

    @app.get("/jobs")
    def list_jobs(directory: ScenarioDir) -> list[str]:
        return [
            job.name
            for name, scenario in _load(directory).items()
            for job in expand_jobs(name, scenario)
        ]

    @app.post("/jobs/{job_name}/runs")
    def launch_job(
        job_name: JobName,
        gateway: Gateway,
        directory: ScenarioDir,
        body: LaunchRequest | None = None,
    ) -> LaunchResponse:
        for name, scenario in _load(directory).items():
            for job in expand_jobs(name, scenario):
                if job.name == job_name:
                    return _launch(gateway, [job], body or LaunchRequest())
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")

    @app.post("/scenarios/{scenario}/runs")
    def launch_scenario(
        scenario: ScenarioName,
        gateway: Gateway,
        directory: ScenarioDir,
        body: LaunchRequest | None = None,
    ) -> LaunchResponse:
        loaded = _load(directory).get(scenario)
        if loaded is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "scenario not found")
        # Leaf jobs already include their upstream jobs' ops; launching the rest would repeat work.
        jobs = leaf_jobs(expand_jobs(scenario, loaded))
        return _launch(gateway, jobs, body or LaunchRequest())

    @app.get("/runs/{run_id}")
    def run_status(run_id: RunId, gateway: Gateway) -> RunStatusResponse:
        return RunStatusResponse(run_id=run_id, status=gateway.status(run_id))

    @app.put("/scenarios/{scenario}", status_code=status.HTTP_200_OK)
    async def put_scenario(
        scenario: ScenarioName,
        request: Request,
        response: Response,
        gateway: Gateway,
        directory: ScenarioDir,
    ) -> ScenarioSummary:
        raw = await _read_limited_body(request, MAX_SCENARIO_FILE_BYTES)
        try:
            parsed = parse_scenario(raw)
        except ValidationError as exc:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                exc.errors(include_url=False, include_input=False, include_context=False),
            ) from exc
        except ScenarioSyntaxError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, "body must be UTF-8 YAML"
            ) from exc

        path = directory / f"{scenario}{SCENARIO_SUFFIX}"
        if not path.match(scenario_glob()):
            raise HTTPException(
                status.HTTP_409_CONFLICT, "scenario would not be loaded by MOCK_IO_SCENARIO_GLOB"
            )
        existed = path.exists()
        await run_in_threadpool(_write_atomically, path, raw)
        await run_in_threadpool(gateway.reload)
        if not existed:
            response.status_code = status.HTTP_201_CREATED
        return _summary(scenario, parsed)

    FastAPIInstrumentor.instrument_app(app)
    return app


app = create_app()


def main() -> None:
    uvicorn.run(
        app,
        host=os.environ.get("MOCK_IO_API_HOST", "127.0.0.1"),
        port=int(os.environ.get("MOCK_IO_API_PORT", "8000")),
    )
