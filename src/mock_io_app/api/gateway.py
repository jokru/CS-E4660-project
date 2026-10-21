import logging
import os
from collections.abc import Mapping

from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError
from dagster_graphql.client.utils import ReloadRepositoryLocationStatus

logger = logging.getLogger(__name__)

_NOT_FOUND_TYPES = {"JobNotFoundError", "PipelineNotFoundError", "RunNotFoundError"}


class GatewayError(Exception):
    def __init__(self, message: str, status_code: int = 502) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class DagsterGateway:
    def __init__(
        self,
        client: DagsterGraphQLClient,
        location_name: str,
        repository_name: str,
    ) -> None:
        self._client = client
        self._location_name = location_name
        self._repository_name = repository_name

    @classmethod
    def from_env(cls) -> "DagsterGateway":
        client = DagsterGraphQLClient(
            os.environ.get("DAGSTER_HOST", "127.0.0.1"),
            port_number=int(os.environ.get("DAGSTER_PORT", "3000")),
            timeout=int(os.environ.get("DAGSTER_TIMEOUT_S", "30")),
        )
        return cls(
            client,
            location_name=os.environ.get("DAGSTER_LOCATION_NAME", "mock_io_app"),
            repository_name=os.environ.get("DAGSTER_REPOSITORY_NAME", "__repository__"),
        )

    def submit(self, job_name: str, tags: Mapping[str, str]) -> str:
        try:
            return self._client.submit_job_execution(
                job_name,
                repository_location_name=self._location_name,
                repository_name=self._repository_name,
                tags=dict(tags),
            )
        except DagsterGraphQLClientError as exc:
            raise _translate(exc, f"failed to submit job {job_name}") from exc

    def status(self, run_id: str) -> str:
        try:
            return self._client.get_run_status(run_id).value
        except DagsterGraphQLClientError as exc:
            raise _translate(exc, f"failed to get status of run {run_id}") from exc

    def reload(self) -> None:
        try:
            info = self._client.reload_repository_location(self._location_name)
        except DagsterGraphQLClientError as exc:
            raise _translate(exc, "failed to reload code location") from exc
        if info.status != ReloadRepositoryLocationStatus.SUCCESS:
            logger.warning("code location reload failed: %s %s", info.failure_type, info.message)
            raise GatewayError(f"code location reload failed: {info.failure_type}")


def _translate(exc: DagsterGraphQLClientError, message: str) -> GatewayError:
    error_type = exc.args[0] if exc.args else ""
    if error_type in _NOT_FOUND_TYPES:
        return GatewayError(f"{message}: not found", status_code=404)
    if error_type == "RunConflict":
        return GatewayError(f"{message}: run conflict", status_code=409)
    # Client messages can embed full queries and variables; keep them out of responses.
    logger.warning("%s: %s", message, exc)
    return GatewayError(f"{message}: Dagster unavailable or returned an error")
