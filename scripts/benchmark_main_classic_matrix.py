#!/usr/bin/env python3

import argparse
import csv
import json
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_csv(value: str) -> list[int]:
    return [int(item) for item in parse_csv(value)]


def cleanup_generated_jobs(namespaces: list[str], timeout_seconds: float = 120.0) -> list[str]:
    errors: list[str] = []
    for namespace in namespaces:
        completed = subprocess.run(
            ["kubectl", "-n", namespace, "get", "jobs", "-o", "name"],
            text=True,
            capture_output=True,
        )
        if completed.returncode != 0:
            errors.append(completed.stderr.strip() or completed.stdout.strip())
            continue
        jobs = [
            line.strip()
            for line in completed.stdout.splitlines()
            if line.strip().startswith("job.batch/jorrit-stutterheim-")
        ]
        if not jobs:
            continue
        delete = subprocess.run(
            ["kubectl", "-n", namespace, "delete", *jobs],
            text=True,
            capture_output=True,
        )
        if delete.returncode != 0:
            errors.append(delete.stderr.strip() or delete.stdout.strip())

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        remaining = 0
        for namespace in namespaces:
            completed = subprocess.run(
                ["kubectl", "-n", namespace, "get", "pods", "-o", "name"],
                text=True,
                capture_output=True,
            )
            if completed.returncode != 0:
                continue
            remaining += sum(
                1
                for line in completed.stdout.splitlines()
                if line.strip().startswith("pod/jorrit-stutterheim-")
            )
        if remaining == 0:
            return errors
        time.sleep(2)

    errors.append("generated job pod cleanup timed out")
    return errors


def rounded(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 3)


def summarize_numbers(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"median": None, "min": None, "max": None}
    return {
        "median": rounded(float(statistics.median(values))),
        "min": rounded(min(values)),
        "max": rounded(max(values)),
    }


def parse_benchmark_output(stdout: str) -> dict[str, Any]:
    stripped = stdout.strip()
    if stripped:
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return {"ok": False, "error": "benchmark produced no output"}
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"benchmark produced invalid JSON: {exc}", "stdout": stdout}


def run_once(
    benchmark_script: Path,
    url: str,
    timeout: int,
    limit: int,
    providers: list[str],
    archetype: str,
    query_shape: str,
    strict: bool,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(benchmark_script),
        "--url",
        url,
        "--limit",
        str(limit),
        "--providers",
        ",".join(providers),
        "--archetype",
        archetype,
        "--query-shape",
        query_shape,
        "--timeout",
        str(timeout),
        "--expected-rows",
        str(limit * len(providers)),
    ]
    if strict:
        command.append("--strict")

    started = time.monotonic()
    completed = subprocess.run(command, text=True, capture_output=True)
    wall_seconds = time.monotonic() - started
    result = parse_benchmark_output(completed.stdout)
    result["returnCode"] = completed.returncode
    result["wallSeconds"] = round(wall_seconds, 3)
    if completed.stderr:
        result["stderr"] = completed.stderr.strip()
    if completed.returncode != 0:
        result["ok"] = False
    return result


def aggregate_group(group: dict[str, Any], runs: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [run for run in runs if run.get("ok")]
    return {
        **group,
        "runs": len(runs),
        "okRuns": len(successful),
        "failedRuns": len(runs) - len(successful),
        "firstResultSeconds": summarize_numbers(
            [run["firstResultSeconds"] for run in successful if run.get("firstResultSeconds") is not None]
        ),
        "doneSeconds": summarize_numbers(
            [run["doneSeconds"] for run in successful if run.get("doneSeconds") is not None]
        ),
        "wallSeconds": summarize_numbers(
            [run["wallSeconds"] for run in successful if run.get("wallSeconds") is not None]
        ),
        "observedRows": {
            "median": statistics.median([run["observedRows"] for run in successful if run.get("observedRows") is not None])
            if any(run.get("observedRows") is not None for run in successful)
            else None,
            "min": min([run["observedRows"] for run in successful if run.get("observedRows") is not None], default=None),
            "max": max([run["observedRows"] for run in successful if run.get("observedRows") is not None], default=None),
        },
        "responseBytes": summarize_numbers(
            [run["responseBytes"] for run in successful if run.get("responseBytes") is not None]
        ),
        "contentResultHashes": sorted({run.get("contentResultHash") for run in successful if run.get("contentResultHash")}),
        "rawResultHashes": sorted({run.get("rawResultHash") for run in successful if run.get("rawResultHash")}),
        "errors": [run.get("errors") or run.get("error") for run in runs if not run.get("ok")],
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, summaries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "dataset",
        "limit",
        "workload",
        "archetype",
        "queryShape",
        "providers",
        "temperature",
        "transport",
        "responseMode",
        "runs",
        "okRuns",
        "failedRuns",
        "firstMedian",
        "firstMin",
        "firstMax",
        "doneMedian",
        "doneMin",
        "doneMax",
        "rowsMedian",
        "responseBytesMedian",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for summary in summaries:
            first = summary["firstResultSeconds"]
            done = summary["doneSeconds"]
            response_bytes = summary["responseBytes"]
            writer.writerow(
                {
                    "dataset": summary["dataset"],
                    "limit": summary["limit"],
                    "workload": summary["workload"],
                    "archetype": summary["archetype"],
                    "queryShape": summary.get("queryShape") or "default",
                    "providers": summary["providersLabel"],
                    "temperature": summary["temperature"],
                    "transport": summary["transport"],
                    "responseMode": summary["responseMode"],
                    "runs": summary["runs"],
                    "okRuns": summary["okRuns"],
                    "failedRuns": summary["failedRuns"],
                    "firstMedian": first["median"],
                    "firstMin": first["min"],
                    "firstMax": first["max"],
                    "doneMedian": done["median"],
                    "doneMin": done["min"],
                    "doneMax": done["max"],
                    "rowsMedian": summary["observedRows"]["median"],
                    "responseBytesMedian": response_bytes["median"],
                }
            )


def write_markdown(path: Path, summaries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| dataset | limit | workload | archetype | query shape | providers | temperature | transport | response mode | ok/runs | first median (min-max) | done median (min-max) | rows | response bytes |",
        "| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for summary in summaries:
        first = summary["firstResultSeconds"]
        done = summary["doneSeconds"]
        response_bytes = summary["responseBytes"]
        lines.append(
            "| {dataset} | {limit} | {workload} | {archetype} | {query_shape} | {providers} | {temperature} | {transport} | {response_mode} | {ok}/{runs} | "
            "{first_median} ({first_min}-{first_max}) | {done_median} ({done_min}-{done_max}) | {rows_median} | {response_bytes} |".format(
                dataset=summary["dataset"],
                limit=summary["limit"],
                workload=summary["workload"],
                archetype=summary["archetype"],
                query_shape=summary.get("queryShape") or "default",
                providers=summary["providersLabel"],
                temperature=summary["temperature"],
                transport=summary["transport"],
                response_mode=summary["responseMode"],
                ok=summary["okRuns"],
                runs=summary["runs"],
                first_median=first["median"],
                first_min=first["min"],
                first_max=first["max"],
                done_median=done["median"],
                done_min=done["min"],
                done_max=done["max"],
                rows_median=summary["observedRows"]["median"],
                response_bytes=response_bytes["median"],
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated main-branch classic DYNAMOS baseline requests.")
    parser.add_argument("--url", default="http://127.0.0.1:18080/api/v1/requestApproval")
    parser.add_argument("--benchmark-script", type=Path)
    parser.add_argument("--limits", default="50000,100000,250000")
    parser.add_argument("--providers", default="UVA")
    parser.add_argument("--archetypes", default="dataThroughTtp")
    parser.add_argument("--query-shapes", default="default")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--name", default=time.strftime("main-classic-%Y%m%d-%H%M%S"))
    parser.add_argument("--temperature", default="warm")
    parser.add_argument("--shuffle-seed", type=int, default=0)
    parser.add_argument("--cooldown-seconds", type=float, default=0.0)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--cleanup-generated-jobs", action="store_true")
    parser.add_argument("--cleanup-namespaces", default="surf,uva,vu")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    benchmark_script = args.benchmark_script or root / "scripts" / "benchmark_main_classic.py"
    providers = parse_csv(args.providers)
    limits = parse_int_csv(args.limits)
    archetypes = parse_csv(args.archetypes)
    query_shapes = parse_csv(args.query_shapes)
    cleanup_namespaces = parse_csv(args.cleanup_namespaces)
    if not providers:
        print(json.dumps({"error": "no providers selected"}))
        return 2
    if not limits:
        print(json.dumps({"error": "no limits selected"}))
        return 2
    if not archetypes:
        print(json.dumps({"error": "no archetypes selected"}))
        return 2
    if not query_shapes:
        print(json.dumps({"error": "no query shapes selected"}))
        return 2

    groups = [
        {
            "dataset": "large",
            "limit": limit,
            "workload": "bulk",
            "archetype": archetype,
            "queryShape": query_shape,
            "providers": providers,
            "providersLabel": ",".join(providers),
            "temperature": args.temperature,
            "transport": "unary",
            "responseMode": "classic-unary",
        }
        for limit in limits
        for archetype in archetypes
        for query_shape in query_shapes
    ]

    all_runs = []
    runs_by_group: dict[tuple[int, str, str], list[dict[str, Any]]] = {
        (group["limit"], group["archetype"], group["queryShape"]): []
        for group in groups
    }
    for repetition in range(1, args.repetitions + 1):
        repetition_groups = list(groups)
        random.Random(args.shuffle_seed + repetition).shuffle(repetition_groups)
        for group in repetition_groups:
            cleanup_before_errors: list[str] = []
            if args.cleanup_generated_jobs:
                cleanup_before_errors = cleanup_generated_jobs(cleanup_namespaces)
                for error in cleanup_before_errors:
                    print(f"cleanup before warning: {error}", file=sys.stderr, flush=True)
            print(
                "running main classic "
                f"limit={group['limit']} archetype={group['archetype']} "
                f"query-shape={group['queryShape']} providers={group['providersLabel']} "
                f"rep={repetition}/{args.repetitions}",
                file=sys.stderr,
                flush=True,
            )
            run = run_once(
                benchmark_script,
                args.url,
                args.timeout,
                group["limit"],
                group["providers"],
                group["archetype"],
                group["queryShape"],
                args.strict,
            )
            run.update(group)
            run["repetition"] = repetition
            if cleanup_before_errors:
                run["cleanupBeforeErrors"] = cleanup_before_errors
            cleanup_after_errors: list[str] = []
            if args.cleanup_generated_jobs:
                cleanup_after_errors = cleanup_generated_jobs(cleanup_namespaces)
                for error in cleanup_after_errors:
                    print(f"cleanup after warning: {error}", file=sys.stderr, flush=True)
            if cleanup_after_errors:
                run["cleanupAfterErrors"] = cleanup_after_errors
            runs_by_group[(group["limit"], group["archetype"], group["queryShape"])].append(run)
            all_runs.append(run)
            if args.cooldown_seconds > 0 and not (
                repetition == args.repetitions and group == repetition_groups[-1]
            ):
                time.sleep(args.cooldown_seconds)

    summaries = [
        aggregate_group(group, runs_by_group[(group["limit"], group["archetype"], group["queryShape"])])
        for group in groups
    ]

    output_dir = args.output_dir
    json_path = output_dir / f"{args.name}.json"
    csv_path = output_dir / f"{args.name}.csv"
    markdown_path = output_dir / f"{args.name}.md"
    write_json(json_path, {"runs": all_runs, "summaries": summaries})
    write_csv(csv_path, summaries)
    write_markdown(markdown_path, summaries)

    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "markdown": str(markdown_path)}))
    return 0 if all(run.get("ok") for run in all_runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
