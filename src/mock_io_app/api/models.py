from pydantic import BaseModel, Field, field_validator

MAX_REPEAT = 100
MAX_RUNS_PER_REQUEST = 1_000
MAX_EXTRA_TAGS = 20
RESERVED_TAG_PREFIXES = ("dagster/", "otel/", "mock_io/")


class LaunchRequest(BaseModel):
    repeat: int = Field(default=1, ge=1, le=MAX_REPEAT)
    tags: dict[str, str] = Field(default_factory=dict, max_length=MAX_EXTRA_TAGS)

    @field_validator("tags")
    @classmethod
    def _check_tags(cls, tags: dict[str, str]) -> dict[str, str]:
        for key, value in tags.items():
            if not key or len(key) > 128 or len(value) > 1024:
                raise ValueError("tag keys must be 1-128 chars and values at most 1024 chars")
            if key.startswith(RESERVED_TAG_PREFIXES):
                raise ValueError(f"tag {key!r} uses a reserved prefix")
        return tags


class LaunchedRun(BaseModel):
    job: str
    run_id: str


class LaunchResponse(BaseModel):
    runs: list[LaunchedRun]


class RunStatusResponse(BaseModel):
    run_id: str
    status: str


class ScenarioSummary(BaseModel):
    name: str
    description: str
    jobs: list[str]
