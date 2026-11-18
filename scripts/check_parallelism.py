"""Report how much work overlapped in time in a trace capture (OTLP JSON lines, e.g.
out/baseline_naive.jsonl): concurrent runs, concurrent jobs inside a run, and gathered calls.

Usage: uv run python scripts/check_parallelism.py out/baseline_naive.jsonl
"""

import json
import sys
from collections import defaultdict

IO_CALLS = ("db.", "gcs.", "http.")


def max_concurrency(intervals: list[tuple[int, int]]) -> int:
    # Ends sort before starts at the same instant, so touching intervals do not count as overlap.
    events = sorted([(a, 1) for a, _ in intervals] + [(b, -1) for _, b in intervals])
    current = peak = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak


def main(path: str) -> None:
    runs: list[tuple[int, int]] = []
    jobs: dict[str, list[tuple[int, int]]] = defaultdict(list)
    calls: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    with open(path) as handle:
        for line in handle:
            for resource in json.loads(line).get("resourceSpans", []):
                for scope in resource.get("scopeSpans", []):
                    for span in scope.get("spans", []):
                        name = span["name"]
                        interval = (int(span["startTimeUnixNano"]), int(span["endTimeUnixNano"]))
                        if name.startswith("run."):
                            runs.append(interval)
                        elif name.startswith("job."):
                            jobs[span["traceId"]].append(interval)
                        elif name.startswith(IO_CALLS) and name.count(".") == 1:
                            calls[(span["traceId"], span.get("parentSpanId", ""))].append(interval)

    multi_job = [max_concurrency(v) for v in jobs.values() if len(v) > 1]
    multi_call = [max_concurrency(v) for v in calls.values() if len(v) > 1]
    print(f"runs: {len(runs)}, max running at once: {max_concurrency(runs)}")
    print(
        f"runs with several jobs: {len(multi_job)}, with overlapping jobs: "
        f"{sum(m > 1 for m in multi_job)}, max jobs at once: {max(multi_job, default=0)}"
    )
    print(
        f"call groups with several calls: {len(multi_call)}, with overlapping calls (gather): "
        f"{sum(m > 1 for m in multi_call)}"
    )


if __name__ == "__main__":
    main(sys.argv[1])
