import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx

DEFAULT_URL = "http://127.0.0.1:8000"
TERMINAL_STATUSES = {"SUCCESS", "FAILURE", "CANCELED"}


class CliError(Exception):
    pass


def _parse_tags(pairs: list[str]) -> dict[str, str]:
    tags: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise CliError(f"tag must be KEY=VALUE, got {pair!r}")
        tags[key] = value
    return tags


def _request(client: httpx.Client, method: str, path: str, **kwargs: Any) -> Any:
    try:
        response = client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        raise CliError(f"cannot reach API: {exc}") from exc
    if response.is_error:
        raise CliError(f"{response.status_code}: {response.text}")
    return response.json()


def _wait_for_runs(client: httpx.Client, run_ids: list[str], poll_s: float) -> bool:
    pending = set(run_ids)
    ok = True
    while pending:
        for run_id in sorted(pending):
            status = _request(client, "GET", f"/runs/{run_id}")["status"]
            if status in TERMINAL_STATUSES:
                pending.discard(run_id)
                ok &= status == "SUCCESS"
                print(f"{run_id}\t{status}")
        if pending:
            time.sleep(poll_s)
    return ok


def _print_launch(client: httpx.Client, result: dict[str, Any], wait: bool, poll_s: float) -> int:
    for run in result["runs"]:
        print(f"{run['job']}\t{run['run_id']}")
    if wait and not _wait_for_runs(client, [r["run_id"] for r in result["runs"]], poll_s):
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mock-io-ctl", description="Client for the mock-io API.")
    parser.add_argument("--url", default=os.environ.get("MOCK_IO_API_URL", DEFAULT_URL))
    parser.add_argument("--timeout", type=float, default=30.0)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("jobs", help="list job names")
    sub.add_parser("scenarios", help="list scenarios and their jobs")

    for name, target in (("run-job", "job"), ("run-scenario", "scenario")):
        cmd = sub.add_parser(name, help=f"launch a {target}")
        cmd.add_argument(target)
        cmd.add_argument("--repeat", type=int, default=1)
        cmd.add_argument("--tag", action="append", default=[], metavar="KEY=VALUE")
        cmd.add_argument("--wait", action="store_true", help="poll until runs finish")
        cmd.add_argument("--poll-interval", type=float, default=1.0)

    status = sub.add_parser("status", help="show run status")
    status.add_argument("run_id")

    upload = sub.add_parser("upload", help="create or replace a scenario from a YAML file")
    upload.add_argument("name")
    upload.add_argument("file", type=Path)
    return parser


def _dispatch(args: argparse.Namespace, client: httpx.Client) -> int:
    match args.command:
        case "jobs":
            print("\n".join(_request(client, "GET", "/jobs")))
        case "scenarios":
            for scenario in _request(client, "GET", "/scenarios"):
                print(
                    f"{scenario['name']}\t{len(scenario['jobs'])} jobs\t{scenario['description']}"
                )
        case "run-job" | "run-scenario":
            kind = "jobs" if args.command == "run-job" else "scenarios"
            body = {"repeat": args.repeat, "tags": _parse_tags(args.tag)}
            result = _request(client, "POST", f"/{kind}/{getattr(args, kind[:-1])}/runs", json=body)
            return _print_launch(client, result, args.wait, args.poll_interval)
        case "status":
            print(_request(client, "GET", f"/runs/{args.run_id}")["status"])
        case "upload":
            result = _request(
                client,
                "PUT",
                f"/scenarios/{args.name}",
                content=args.file.read_bytes(),
                headers={"content-type": "application/yaml"},
            )
            print(json.dumps(result, indent=2))
    return 0


def main(argv: list[str] | None = None, transport: httpx.BaseTransport | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        with httpx.Client(base_url=args.url, timeout=args.timeout, transport=transport) as client:
            return _dispatch(args, client)
    except (CliError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
