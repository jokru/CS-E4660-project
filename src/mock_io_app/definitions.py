from pathlib import Path

from dagster import Definitions

from mock_io_app.job_factory import build_job, default_resources
from mock_io_app.scenario import expand_jobs, load_scenarios
from mock_io_app.tracing import setup_tracing


def build_definitions(directory: Path | None = None, glob: str | None = None) -> Definitions:
    expanded_jobs = [
        expanded
        for name, scenario in load_scenarios(directory, glob).items()
        for expanded in expand_jobs(name, scenario)
    ]
    by_name = {expanded.name: expanded for expanded in expanded_jobs}
    jobs = [build_job(expanded, by_name) for expanded in expanded_jobs]
    return Definitions(jobs=jobs, resources=default_resources())


setup_tracing("mock-io-dagster")
defs = build_definitions()
