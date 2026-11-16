import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from mock_io_app.latency import MAX_LATENCY_MS, FixedLatency, LatencySpec

NAME_PATTERN = r"^[a-z0-9_]{1,64}$"
SERVICE_NAME_PATTERN = r"^[a-z0-9_-]{1,64}$"
MAX_COPIES = 1_000
MAX_ANOMALIES = 10
MAX_CALL_COUNT = 10_000
MAX_CONCURRENCY = 1_000
MAX_PAYLOAD_BYTES = 10 * 1024 * 1024
MAX_RETRIES = 10
MAX_DEPENDENCIES = 50
MAX_SERVICES = 100
MAX_OPS_PER_JOB = 2_000
MAX_TOTAL_OPS = 50_000
MAX_TOTAL_CALLS = 1_000_000
MAX_SCENARIO_FILE_BYTES = 1024 * 1024

IOType = Literal["db", "gcs", "http", "service"]
ALLOWED_ACTIONS: dict[str, frozenset[str]] = {
    "db": frozenset({"query", "insert"}),
    "gcs": frozenset({"upload", "download", "list"}),
    "http": frozenset({"get", "post"}),
    "service": frozenset({"call"}),
}

DEFAULT_SCENARIO_DIR = Path(__file__).resolve().parents[2] / "scenarios"


class _Spec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Defaults(_Spec):
    # None -> nondeterministic latencies/failures
    seed: int | None = 0
    latency: LatencySpec = FixedLatency(dist="fixed", ms=10.0)
    failure_rate: float = Field(default=0.0, ge=0, le=1)


class AnomalySpec(_Spec):
    """A labelled deviation applied to a random subset of calls (ground truth for evaluation)."""

    label: str = Field(pattern=NAME_PATTERN)
    # Chance that a call is affected.
    probability: float = Field(default=1.0, ge=0, le=1)
    latency_multiplier: float = Field(default=1.0, ge=0, le=1_000)
    latency_add_ms: float = Field(default=0.0, ge=0, le=MAX_LATENCY_MS)
    # Overrides the group's payload size.
    payload_bytes: int | None = Field(default=None, ge=0, le=MAX_PAYLOAD_BYTES)
    fail: bool = False
    # Active for run iterations in [from_iteration, to_iteration); to_iteration None = open-ended.
    from_iteration: int = Field(default=0, ge=0)
    to_iteration: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _check(self) -> "AnomalySpec":
        if (
            self.latency_multiplier == 1.0
            and self.latency_add_ms == 0
            and self.payload_bytes is None
            and not self.fail
        ):
            raise ValueError(f"anomaly {self.label!r} has no effect")
        if self.to_iteration is not None and self.to_iteration <= self.from_iteration:
            raise ValueError("to_iteration must be greater than from_iteration")
        return self

    def active(self, iteration: int) -> bool:
        return iteration >= self.from_iteration and (
            self.to_iteration is None or iteration < self.to_iteration
        )


class CallGroupSpec(_Spec):
    io: IOType
    action: str
    # Name of an entry in the scenario's `services`; required for (and only for) io=service.
    service: str | None = Field(default=None, pattern=NAME_PATTERN)
    count: int = Field(default=1, ge=1, le=MAX_CALL_COUNT)
    mode: Literal["sequential", "gather"] = "sequential"
    # None -> unbounded within the group
    max_concurrency: int | None = Field(default=None, ge=1, le=MAX_CONCURRENCY)
    latency: LatencySpec | None = None
    failure_rate: float | None = Field(default=None, ge=0, le=1)
    payload_bytes: int = Field(default=0, ge=0, le=MAX_PAYLOAD_BYTES)
    anomalies: list[AnomalySpec] = Field(default_factory=list, max_length=MAX_ANOMALIES)

    @model_validator(mode="after")
    def _check_action(self) -> "CallGroupSpec":
        allowed = ALLOWED_ACTIONS[self.io]
        if self.action not in allowed:
            raise ValueError(
                f"action {self.action!r} not allowed for io {self.io!r}; "
                f"expected one of {sorted(allowed)}"
            )
        if (self.io == "service") != (self.service is not None):
            raise ValueError("`service` must be set if and only if io is 'service'")
        return self


class ServiceSpec(_Spec):
    # first_party: its internal call_groups run (and are traced) inside the call.
    # third_party: a black box; only the client-side call is traced.
    kind: Literal["first_party", "third_party"]
    call_groups: list[CallGroupSpec] = Field(default_factory=list)
    # Applied to every call of this service (in addition to the calling group's own anomalies).
    anomalies: list[AnomalySpec] = Field(default_factory=list, max_length=MAX_ANOMALIES)

    @model_validator(mode="after")
    def _check_groups(self) -> "ServiceSpec":
        if self.kind == "third_party" and self.call_groups:
            raise ValueError("third_party services are black boxes and cannot define call_groups")
        if self.kind == "first_party" and not self.call_groups:
            raise ValueError("first_party services must define call_groups")
        return self


class OpSpec(_Spec):
    name: str = Field(pattern=NAME_PATTERN)
    retries: int = Field(default=0, ge=0, le=MAX_RETRIES)
    call_groups: list[CallGroupSpec] = Field(min_length=1)


class JobSpec(_Spec):
    name: str = Field(pattern=NAME_PATTERN)
    copies: int = Field(default=1, ge=1, le=MAX_COPIES)
    executor: Literal["in_process", "multiprocess"] = "in_process"
    # Overrides the scenario's service_name for this job's op spans.
    service_name: str | None = Field(default=None, pattern=SERVICE_NAME_PATTERN)
    # Base job names (before copies expansion). The job's Dagster run includes the ops of every
    # copy of each dependency, which must finish before this job's first op starts.
    depends_on: list[str] = Field(default_factory=list, max_length=MAX_DEPENDENCIES)
    ops: list[OpSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_ops(self) -> "JobSpec":
        names = [op.name for op in self.ops]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate op names in job {self.name!r}")
        return self

    def calls_per_run(self, service_calls: Mapping[str, int] | None = None) -> int:
        return sum(_group_calls(g, service_calls) for op in self.ops for g in op.call_groups)


class Scenario(_Spec):
    description: str = ""
    # `service.name` of the root job span and of ops in jobs without their own service_name.
    service_name: str | None = Field(default=None, pattern=SERVICE_NAME_PATTERN)
    defaults: Defaults = Defaults()
    services: dict[str, ServiceSpec] = Field(default_factory=dict, max_length=MAX_SERVICES)
    jobs: list[JobSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_jobs(self) -> "Scenario":
        for name in self.services:
            if not is_valid_name(name):
                raise ValueError(f"service name {name!r} must match {NAME_PATTERN}")
        service_calls = _service_call_counts(self.services)
        for job in self.jobs:
            for op in job.ops:
                _check_service_refs(op.call_groups, self.services, f"job {job.name!r}")
        expanded = [name for job in self.jobs for name in expanded_job_names(job)]
        if len(expanded) != len(set(expanded)):
            raise ValueError("job names collide after copies expansion")
        total = sum(job.copies * job.calls_per_run(service_calls) for job in self.jobs)
        if total > MAX_TOTAL_CALLS:
            raise ValueError(f"scenario defines {total} calls, max is {MAX_TOTAL_CALLS}")
        _check_dependencies(self.jobs)
        return self


def _group_calls(group: CallGroupSpec, service_calls: Mapping[str, int] | None) -> int:
    # A service call counts itself plus everything its handler does.
    inner = (service_calls or {}).get(group.service or "", 0)
    return group.count * (1 + inner)


def _check_service_refs(
    groups: list[CallGroupSpec], services: Mapping[str, ServiceSpec], where: str
) -> None:
    for group in groups:
        if group.service is not None and group.service not in services:
            raise ValueError(f"{where} calls unknown service {group.service!r}")


def _service_call_counts(services: Mapping[str, ServiceSpec]) -> dict[str, int]:
    """Total calls one invocation of each service performs inside; rejects unknown refs and cycles."""
    counts: dict[str, int] = {}
    visiting: set[str] = set()

    def visit(name: str) -> int:
        if name in counts:
            return counts[name]
        if name in visiting:
            raise ValueError(f"service call cycle involving {name!r}")
        visiting.add(name)
        spec = services[name]
        _check_service_refs(spec.call_groups, services, f"service {name!r}")
        total = sum(
            g.count * (1 + (visit(g.service) if g.service else 0)) for g in spec.call_groups
        )
        visiting.discard(name)
        if total > MAX_TOTAL_CALLS:
            raise ValueError(f"service {name!r} expands to more than {MAX_TOTAL_CALLS} calls")
        counts[name] = total
        return total

    for name in services:
        visit(name)
    return counts


def _check_dependencies(jobs: list[JobSpec]) -> None:
    graph = {job.name: job.depends_on for job in jobs}
    for name, deps in graph.items():
        if len(deps) != len(set(deps)):
            raise ValueError(f"job {name!r} lists a dependency twice")
        for dep in deps:
            if dep == name:
                raise ValueError(f"job {name!r} depends on itself")
            if dep not in graph:
                raise ValueError(f"job {name!r} depends on unknown job {dep!r}")
    remaining = {name: set(deps) for name, deps in graph.items()}
    while remaining:
        ready = [name for name, deps in remaining.items() if not deps]
        if not ready:
            raise ValueError(f"dependency cycle among jobs {sorted(remaining)}")
        for name in ready:
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)

    by_name = {job.name: job for job in jobs}
    sizes: dict[str, int] = {}

    # Upper bound: shared upstreams are counted once per path.
    def size(name: str) -> int:
        if name not in sizes:
            job = by_name[name]
            sizes[name] = len(job.ops) + sum(by_name[d].copies * size(d) for d in job.depends_on)
        return sizes[name]

    for job in jobs:
        if size(job.name) > MAX_OPS_PER_JOB:
            raise ValueError(f"job {job.name!r} expands to more than {MAX_OPS_PER_JOB} ops")
    if sum(job.copies * size(job.name) for job in jobs) > MAX_TOTAL_OPS:
        raise ValueError(f"scenario expands to more than {MAX_TOTAL_OPS} ops")


@dataclass(frozen=True)
class ExpandedJob:
    scenario: str
    name: str
    spec: JobSpec
    defaults: Defaults
    # Expanded names of every upstream job copy whose ops run before this job's ops.
    upstream: tuple[str, ...] = ()
    services: Mapping[str, ServiceSpec] = field(default_factory=dict)
    service_name: str | None = None
    # The scenario-level name, used for the root `job.*` span.
    scenario_service_name: str | None = None


def expanded_job_names(job: JobSpec) -> list[str]:
    if job.copies == 1:
        return [job.name]
    return [f"{job.name}_{i}" for i in range(job.copies)]


def expand_jobs(scenario_name: str, scenario: Scenario) -> list[ExpandedJob]:
    by_base = {job.name: job for job in scenario.jobs}
    return [
        ExpandedJob(
            scenario=scenario_name,
            name=f"{scenario_name}__{name}",
            spec=job,
            defaults=scenario.defaults,
            services=scenario.services,
            service_name=job.service_name or scenario.service_name,
            scenario_service_name=scenario.service_name,
            upstream=tuple(
                f"{scenario_name}__{up}"
                for dep in job.depends_on
                for up in expanded_job_names(by_base[dep])
            ),
        )
        for job in scenario.jobs
        for name in expanded_job_names(job)
    ]


def closure(job: ExpandedJob, by_name: dict[str, ExpandedJob]) -> list[ExpandedJob]:
    """The job and all transitive upstream jobs, upstream first."""
    ordered: list[ExpandedJob] = []
    seen: set[str] = set()

    def visit(current: ExpandedJob) -> None:
        if current.name in seen:
            return
        seen.add(current.name)
        for name in current.upstream:
            visit(by_name[name])
        ordered.append(current)

    visit(job)
    return ordered


def leaf_jobs(jobs: list[ExpandedJob]) -> list[ExpandedJob]:
    """Jobs nothing else depends on; running them covers every job in their closure."""
    depended_on = {name for job in jobs for name in job.upstream}
    return [job for job in jobs if job.name not in depended_on]


class ScenarioSyntaxError(ValueError):
    pass


def parse_scenario(raw: str | bytes) -> Scenario:
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        # Aliases are rejected up front: nested aliases can expand a small file into gigabytes.
        if any(isinstance(e, yaml.AliasEvent) for e in yaml.parse(text, Loader=yaml.SafeLoader)):
            raise ScenarioSyntaxError("YAML anchors and aliases are not supported")
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        problem = getattr(exc, "problem", None) or "syntax error"
        mark = getattr(exc, "problem_mark", None)
        where = f" at line {mark.line + 1}" if mark else ""
        raise ScenarioSyntaxError(f"invalid YAML{where}: {problem}") from exc
    if not isinstance(data, dict):
        raise ScenarioSyntaxError("scenario must be a YAML mapping")
    return Scenario.model_validate(data)


def load_scenario(path: Path) -> Scenario:
    return parse_scenario(path.read_bytes())


def scenario_dir() -> Path:
    return Path(os.environ.get("MOCK_IO_SCENARIO_DIR", DEFAULT_SCENARIO_DIR))


SCENARIO_SUFFIX = ".yaml"


def scenario_glob() -> str:
    return os.environ.get("MOCK_IO_SCENARIO_GLOB", f"*{SCENARIO_SUFFIX}")


def load_scenarios(directory: Path | None = None, glob: str | None = None) -> dict[str, Scenario]:
    directory = directory or scenario_dir()
    scenarios: dict[str, Scenario] = {}
    for path in sorted(directory.glob(glob or scenario_glob())):
        try:
            scenarios[_scenario_name(path)] = load_scenario(path)
        except ValueError as exc:
            raise ValueError(f"invalid scenario file {path}: {exc}") from exc
    return scenarios


def is_valid_name(name: str) -> bool:
    return re.fullmatch(NAME_PATTERN, name) is not None


def _scenario_name(path: Path) -> str:
    if not is_valid_name(path.stem):
        raise ValueError(f"scenario file name must match {NAME_PATTERN}")
    return path.stem
