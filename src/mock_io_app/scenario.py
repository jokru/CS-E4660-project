import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from mock_io_app.latency import FixedLatency, LatencySpec

NAME_PATTERN = r"^[a-z0-9_]{1,64}$"
MAX_COPIES = 1_000
MAX_CALL_COUNT = 10_000
MAX_CONCURRENCY = 1_000
MAX_PAYLOAD_BYTES = 10 * 1024 * 1024
MAX_RETRIES = 10
MAX_DEPENDENCIES = 50
MAX_OPS_PER_JOB = 2_000
MAX_TOTAL_OPS = 50_000
MAX_TOTAL_CALLS = 1_000_000
MAX_SCENARIO_FILE_BYTES = 1024 * 1024

IOType = Literal["db", "gcs", "http"]
ALLOWED_ACTIONS: dict[str, frozenset[str]] = {
    "db": frozenset({"query", "insert"}),
    "gcs": frozenset({"upload", "download", "list"}),
    "http": frozenset({"get", "post"}),
}

DEFAULT_SCENARIO_DIR = Path(__file__).resolve().parents[2] / "scenarios"


class _Spec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Defaults(_Spec):
    # None -> nondeterministic latencies/failures
    seed: int | None = 0
    latency: LatencySpec = FixedLatency(dist="fixed", ms=10.0)
    failure_rate: float = Field(default=0.0, ge=0, le=1)


class CallGroupSpec(_Spec):
    io: IOType
    action: str
    count: int = Field(default=1, ge=1, le=MAX_CALL_COUNT)
    mode: Literal["sequential", "gather"] = "sequential"
    # None -> unbounded within the group
    max_concurrency: int | None = Field(default=None, ge=1, le=MAX_CONCURRENCY)
    latency: LatencySpec | None = None
    failure_rate: float | None = Field(default=None, ge=0, le=1)
    payload_bytes: int = Field(default=0, ge=0, le=MAX_PAYLOAD_BYTES)

    @model_validator(mode="after")
    def _check_action(self) -> "CallGroupSpec":
        allowed = ALLOWED_ACTIONS[self.io]
        if self.action not in allowed:
            raise ValueError(
                f"action {self.action!r} not allowed for io {self.io!r}; "
                f"expected one of {sorted(allowed)}"
            )
        return self


class OpSpec(_Spec):
    name: str = Field(pattern=NAME_PATTERN)
    retries: int = Field(default=0, ge=0, le=MAX_RETRIES)
    call_groups: list[CallGroupSpec] = Field(min_length=1)


class JobSpec(_Spec):
    name: str = Field(pattern=NAME_PATTERN)
    copies: int = Field(default=1, ge=1, le=MAX_COPIES)
    executor: Literal["in_process", "multiprocess"] = "in_process"
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

    def calls_per_run(self) -> int:
        return sum(g.count for op in self.ops for g in op.call_groups)


class Scenario(_Spec):
    description: str = ""
    defaults: Defaults = Defaults()
    jobs: list[JobSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_jobs(self) -> "Scenario":
        expanded = [name for job in self.jobs for name in expanded_job_names(job)]
        if len(expanded) != len(set(expanded)):
            raise ValueError("job names collide after copies expansion")
        total = sum(job.copies * job.calls_per_run() for job in self.jobs)
        if total > MAX_TOTAL_CALLS:
            raise ValueError(f"scenario defines {total} calls, max is {MAX_TOTAL_CALLS}")
        _check_dependencies(self.jobs)
        return self


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
