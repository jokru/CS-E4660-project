# Adaptive Tracing and Trace Reduction Test Bench

The goal of this project is to test and compare different adaptive tracing and trace reduction methods (for example sampling strategies and per-component tracing parameters). To get realistic, repeatable trace data, it is built on `mock-io-app`: a Dagster app whose jobs perform mocked I/O (database, GCS, HTTP and microservice calls) described by YAML scenarios. Every call emits OpenTelemetry spans, so the same workload can be replayed against different collectors and reduction methods.

- [Mock I/O app](#mock-io-app): the workload generator (implemented).
- [Collectors and reduction methods](#collectors-and-reduction-methods): the systems under test (pending).

# Mock I/O app

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```sh
uv sync
uv run pytest
```

## Scenarios

Scenario files live in `scenarios/` (override with `MOCK_IO_SCENARIO_DIR` and `MOCK_IO_SCENARIO_GLOB`). The file name is the scenario name; jobs are exposed as `<scenario>__<job>`.

| File | Contents |
| --- | --- |
| `small.yaml`, `mixed.yaml` | Small examples: sequential and gathered I/O, retries, copies, multiprocess executor |
| `microservices.yaml` | First-party and third-party service calls |
| `supermarket.yaml` | 49-job supermarket data platform: importers, transformers, loaders (see below) |
| `stress/stress.yaml` | High-volume load, loaded only with `MOCK_IO_SCENARIO_DIR=scenarios/stress` |

Structure of a scenario:

- `service_name` (optional): the `service.name` of the root `job.*` span and of ops in jobs without their own `service_name`; default is the process's name (e.g. `mock-io-cli`). A job's `service_name` sets the service of its own ops. Allowed characters: `a-z0-9_-`.
- `defaults`: `seed` (null for nondeterministic), `latency` (`fixed`, `normal`, `uniform`, `lognormal`), `failure_rate`.
- `jobs`: `name`, `copies`, `executor` (`in_process` or `multiprocess`), `depends_on` (base job names; upstream ops run first and are part of the job's run), and `ops`.
- `ops`: `name`, `retries`, and `call_groups`.
- `call_groups`: `io` + `action` (`db`: query/insert, `gcs`: upload/download/list, `http`: get/post, `service`: call), plus `count`, `mode` (`sequential` or `gather`), `max_concurrency`, `latency`, `failure_rate`, `payload_bytes`.
- YAML anchors and aliases are rejected; size limits are defined in `src/mock_io_app/scenario.py`.

### Anomalies

`anomalies` (on a call group, or on a service where it applies to every call of that service) injects labelled deviations into a random subset of calls, so there is ground truth for judging sampling and reduction methods:

```yaml
anomalies:
- label: slow_tax_service      # written to the span as `mock_io.anomaly`
  probability: 0.05            # chance per call (default 1.0)
  latency_multiplier: 8        # and/or latency_add_ms
  # fail: true                 # force a failure
  # payload_bytes: 8388608     # override the payload size
  # from_iteration: 5          # active for iterations in [from, to); `--repeat` / API `repeat` count iterations
  # to_iteration: 8
```

At least one effect is required. Several matching anomalies combine and their labels are comma-separated. Scenarios without anomalies behave exactly as before. The supermarket scenario defines (see `SERVICE_ANOMALIES` and `GROUP_ANOMALIES` in the generator):

| Label | Where | Effect | When |
| --- | --- | --- | --- |
| `slow_tax_service`, `slow_weather_api`, `slow_price_engine` | service calls | 20-30x latency or +1.5 s | rare, random |
| `huge_blob` | GCS uploads | 8 MiB payload, 15x latency | rare, random |
| `http_flap` | HTTP gets | failures, triggering op retries and larger traces | iterations 3-4 |
| `email_outage` | `email_provider` | failures with retries | iterations 6-8 |

Use `--repeat 10` or more to cover the windows. Latency anomalies are only visible in span durations if `--latency-scale` is not tiny (0.01 or higher).

#### Ground truth

The span attribute is lost if a collector drops the span, so for evaluation write an independent log:

- `MOCK_IO_GROUND_TRUTH_FILE=<path>`: appends one JSON line per anomalous call: `trace_id`, `span_id`, `span_name`, `call_index`, `anomalies`, `timestamp`. Join it to the reduced output on `trace_id` / `span_id`. Requires an enabled trace exporter (`console` or `otlp`), since the IDs come from the spans. Delete the file between experiments.
- `MOCK_IO_ANOMALY_ATTRIBUTE=0`: stop exporting `mock_io.anomaly`, so methods under test cannot see the labels (default: exported).

### Microservices

Top-level `services` declares callable services; call groups use `io: service`, `service: <name>`, `action: call`.

- `first_party`: defines its own `call_groups` (which may call other services). The call produces a client span, and the handler's work runs in a nested server span.
- `third_party`: a black box with no internals; only the client-side call span is traced. The call group's latency and failure rate model it.

### Supermarket scenario

Generated by `scripts/generate_supermarket.py` (seeded) into `scenarios/supermarket.yaml`:

- Traces use service name `supermarket-dag` for the root job span, and each job's ops use their own service name (the job name with `-`, e.g. `import-orders`).
- 10 importers (5-10 DB, HTTP and GCS ops each), running independently.
- 15 transformers plus 2 big ones, depending on combinations of importers.
- 20 loaders plus 2 big ones, depending on transformers and importers.
- Big jobs have 10x the steps. Transformers and loaders also call first- and third-party services.

Regenerate with `uv run python scripts/generate_supermarket.py`.

## Running

Run jobs in-process, with no webserver. Runs use a throwaway Dagster instance and don't appear in the Dagster UI unless `DAGSTER_HOME` is set (use the same directory as `dagster dev`, see below). Without `--job`, only leaf jobs run since they include their upstream jobs.

```sh
uv run mock-io-run --scenario scenarios/small.yaml
uv run mock-io-run --scenario scenarios/supermarket.yaml --latency-scale 0.01
uv run mock-io-run --scenario scenarios/supermarket.yaml --job load_store_dashboards --latency-scale 0.01
```

Dagster UI (`DAGSTER_HOME` must be set to a writable directory):

```sh
mkdir -p .dagster_home
DAGSTER_HOME="$PWD/.dagster_home" uv run dagster dev -p 3000
```

The UI is at <http://127.0.0.1:3000> (code location `mock_io_app`). Jobs are named `<scenario>__<job>`; start one from the job's Launchpad ("Launch run"). To export traces to Jaeger, set the tracing variables (see [Emitted traces](#emitted-traces)) in the same shell before starting `dagster dev`, e.g. `MOCK_IO_TRACE_EXPORTER=otlp`.

HTTP API and client:

```sh
uv run mock-io-api                      # MOCK_IO_API_HOST / MOCK_IO_API_PORT, default 127.0.0.1:8000
uv run mock-io-ctl scenarios
uv run mock-io-ctl run-scenario mixed --repeat 3 --wait
uv run mock-io-ctl upload my_scenario my_scenario.yaml
```

## Emitted traces

`MOCK_IO_TRACE_EXPORTER` selects the exporter: `none` (default), `console` or `otlp`. For `otlp`, the standard `OTEL_EXPORTER_OTLP_*` variables apply, and `OTEL_SERVICE_NAME` overrides the service name. Trace context is propagated from the API or CLI into Dagster runs through run tags.

### Local Jaeger

Run Jaeger all-in-one with Docker (OTLP gRPC on 4317, OTLP HTTP on 4318, UI on 16686):

```sh
docker run --rm --name jaeger \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  jaegertracing/jaeger:latest
```

Point the app at it (these are the defaults in `.env`):

```sh
export MOCK_IO_TRACE_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
uv run mock-io-run --scenario scenarios/small.yaml
```

To use OTLP over HTTP instead, set `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`. Browse traces at <http://127.0.0.1:16686>.

Span layout: `run.<job>` (CLI only; scenario `service_name`) > `job.<job>` (one per job in the run, in that job's service; upstream jobs get their own) > `op.<name>` > `call_group.<io>.<action>` > `<io>.<action>` (client span per call). Each job is one Dagster op (so the Dagster graph mirrors the job DAG, and independent jobs can run in parallel with the multiprocess executor); its scenario ops run in order inside the job, and `retries` re-run an op inside the job span (one `op.<name>` span per attempt, `dagster.retry_number`). Service calls produce `service.<name>.call` (client) and, for first-party services, a nested `service.<name>.handle` (server). The handle span and everything under it are exported with `service.name` set to the first-party service's name, so Jaeger shows them as separate services; third-party calls only appear as client spans of the caller.

## Layout

```
src/mock_io_app/
  scenario.py       scenario models, validation, job expansion
  executor.py       runs call groups (sequential/gather, service handlers)
  job_factory.py    builds Dagster jobs and ops from scenarios
  resources/        fake db, gcs, http, service clients
  tracing.py        OpenTelemetry setup and context propagation
  api/              FastAPI app, Dagster gateway, mock-io-ctl client
scenarios/          bundled scenarios
scripts/            scenario generators
tests/
```

# Collectors and reduction methods

**Pending.** Nothing here is implemented yet. The mock I/O app above only produces traces (export via `otlp`); it contains no collector, sampling or reduction logic.

For future plans, see `PROJECT.md`.


# AI Usage

AI coding assistants (GitHub Copilot, using Claude models) were used extensively to implement this project's code, including the scenario model and validation, the executor and job factory, the API and clients, the scenario generator, the tests and this README. The author directed the work (requirements, design decisions and review of the results), and the generated code was checked with the automated test suite and sample runs.
