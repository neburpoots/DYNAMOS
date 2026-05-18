#!/usr/bin/env python3

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


DEFAULT_TRANSPORTS = "unary,streaming,rabbitmq-streams"
DEFAULT_DATASETS = "large,original"
DEFAULT_ARCHETYPES = "dataThroughTtp,computeToData"
DEFAULT_LARGE_LIMITS = "50000,250000"
DEFAULT_PROVIDERS = "UVA,VU"


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_csv(value: str) -> list[int]:
    return [int(item) for item in parse_csv(value)]


def median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def rounded(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 3)


def summarize_numbers(values: list[float]) -> dict[str, float | None]:
    return {
        "median": rounded(median(values)),
        "min": rounded(min(values)) if values else None,
        "max": rounded(max(values)) if values else None,
    }


def benchmark_cases(datasets: list[str], large_limits: list[int], original_limit: int) -> list[tuple[str, int]]:
    cases: list[tuple[str, int]] = []
    for dataset in datasets:
        if dataset == "large":
            cases.extend(("large", limit) for limit in large_limits)
        elif dataset == "original":
            cases.append(("original", original_limit))
        else:
            raise ValueError(f"unknown dataset {dataset!r}")
    return cases


def expected_rows(dataset: str, limit: int, providers: list[str]) -> int | None:
    if dataset != "large":
        return None
    return limit * len(providers)


def parse_benchmark_output(stdout: str) -> dict[str, Any]:
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
    dataset: str,
    limit: int,
    archetype: str,
    transport: str,
    providers: list[str],
    strict: bool,
    require_partial: bool,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(benchmark_script),
        "--url",
        url,
        "--transport",
        transport,
        "--dataset",
        dataset,
        "--limit",
        str(limit),
        "--archetype",
        archetype,
        "--providers",
        ",".join(providers),
        "--timeout",
        str(timeout),
    ]

    rows = expected_rows(dataset, limit, providers)
    if rows is not None:
        command.extend(["--expected-rows", str(rows)])
    if strict:
        command.append("--strict")
    if require_partial:
        command.append("--require-partial")

    started = time.monotonic()
    completed = subprocess.run(command, text=True, capture_output=True)
    elapsed = time.monotonic() - started
    result = parse_benchmark_output(completed.stdout)
    result["returnCode"] = completed.returncode
    result["wallSeconds"] = round(elapsed, 3)
    if completed.stderr:
        result["stderr"] = completed.stderr.strip()
    if completed.returncode != 0:
        result["ok"] = False
    return result


def aggregate_group(group: dict[str, Any], runs: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [run for run in runs if run.get("ok")]
    first_values = [run["firstResultSeconds"] for run in successful if run.get("firstResultSeconds") is not None]
    done_values = [run["doneSeconds"] for run in successful if run.get("doneSeconds") is not None]
    observed_rows = [run["observedRows"] for run in successful if run.get("observedRows") is not None]
    hashes = sorted({run.get("finalResultHash") for run in successful if run.get("finalResultHash")})
    return {
        **group,
        "runs": len(runs),
        "okRuns": len(successful),
        "failedRuns": len(runs) - len(successful),
        "firstResultSeconds": summarize_numbers(first_values),
        "doneSeconds": summarize_numbers(done_values),
        "observedRows": {
            "median": median(observed_rows),
            "min": min(observed_rows) if observed_rows else None,
            "max": max(observed_rows) if observed_rows else None,
        },
        "finalResultHashes": hashes,
        "errors": [run.get("errors") or run.get("error") for run in runs if not run.get("ok")],
    }


def mark_cross_transport_mismatches(summaries: list[dict[str, Any]]) -> None:
    groups: dict[tuple[Any, ...], set[str]] = {}
    for summary in summaries:
        key = (
            summary["dataset"],
            summary["limit"],
            summary["archetype"],
            summary.get("sqlBatchRows"),
            summary.get("rabbitmqChunkRows"),
        )
        groups.setdefault(key, set()).update(summary.get("finalResultHashes", []))

    for summary in summaries:
        key = (
            summary["dataset"],
            summary["limit"],
            summary["archetype"],
            summary.get("sqlBatchRows"),
            summary.get("rabbitmqChunkRows"),
        )
        summary["crossTransportResultMatch"] = len(groups.get(key, set())) <= 1


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, summaries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "dataset",
        "limit",
        "archetype",
        "transport",
        "sqlBatchRows",
        "rabbitmqChunkRows",
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
        "crossTransportResultMatch",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(
                {
                    "dataset": summary["dataset"],
                    "limit": summary["limit"],
                    "archetype": summary["archetype"],
                    "transport": summary["transport"],
                    "sqlBatchRows": summary.get("sqlBatchRows"),
                    "rabbitmqChunkRows": summary.get("rabbitmqChunkRows"),
                    "runs": summary["runs"],
                    "okRuns": summary["okRuns"],
                    "failedRuns": summary["failedRuns"],
                    "firstMedian": summary["firstResultSeconds"]["median"],
                    "firstMin": summary["firstResultSeconds"]["min"],
                    "firstMax": summary["firstResultSeconds"]["max"],
                    "doneMedian": summary["doneSeconds"]["median"],
                    "doneMin": summary["doneSeconds"]["min"],
                    "doneMax": summary["doneSeconds"]["max"],
                    "rowsMedian": summary["observedRows"]["median"],
                    "crossTransportResultMatch": summary["crossTransportResultMatch"],
                }
            )


def write_markdown(path: Path, summaries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| dataset | limit | archetype | transport | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | result match |",
        "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for summary in summaries:
        first = summary["firstResultSeconds"]
        done = summary["doneSeconds"]
        rows = summary["observedRows"]
        lines.append(
            "| {dataset} | {limit} | {archetype} | {transport} | {batch} | {chunk} | {ok}/{runs} | "
            "{first_median} ({first_min}-{first_max}) | {done_median} ({done_min}-{done_max}) | {rows_median} | {match} |".format(
                dataset=summary["dataset"],
                limit=summary["limit"],
                archetype=summary["archetype"],
                transport=summary["transport"],
                batch=summary.get("sqlBatchRows") or "",
                chunk=summary.get("rabbitmqChunkRows") or "",
                ok=summary["okRuns"],
                runs=summary["runs"],
                first_median=first["median"],
                first_min=first["min"],
                first_max=first["max"],
                done_median=done["median"],
                done_min=done["min"],
                done_max=done["max"],
                rows_median=rows["median"],
                match="yes" if summary["crossTransportResultMatch"] else "no",
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated DYNAMOS NDJSON benchmark matrices.")
    parser.add_argument("--url", default="http://127.0.0.1:18080/api/v1/requestApproval")
    parser.add_argument("--benchmark-script", type=Path)
    parser.add_argument("--transports", default=DEFAULT_TRANSPORTS)
    parser.add_argument("--datasets", default=DEFAULT_DATASETS)
    parser.add_argument("--large-limits", default=DEFAULT_LARGE_LIMITS)
    parser.add_argument("--original-limit", type=int, default=1_000_000)
    parser.add_argument("--archetypes", default=DEFAULT_ARCHETYPES)
    parser.add_argument("--providers", default=DEFAULT_PROVIDERS)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--sql-batch-rows")
    parser.add_argument("--rabbitmq-chunk-rows")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--name", default=time.strftime("matrix-%Y%m%d-%H%M%S"))
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--require-partial", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    benchmark_script = args.benchmark_script or root / "scripts" / "benchmark_ndjson.py"
    transports = parse_csv(args.transports)
    datasets = parse_csv(args.datasets)
    archetypes = parse_csv(args.archetypes)
    providers = parse_csv(args.providers)
    large_limits = parse_int_csv(args.large_limits)
    cases = benchmark_cases(datasets, large_limits, args.original_limit)

    all_runs = []
    summaries = []
    for dataset, limit in cases:
        for archetype in archetypes:
            for transport in transports:
                group = {
                    "dataset": dataset,
                    "limit": limit,
                    "archetype": archetype,
                    "transport": transport,
                    "sqlBatchRows": args.sql_batch_rows,
                    "rabbitmqChunkRows": args.rabbitmq_chunk_rows,
                }
                runs = []
                for repetition in range(1, args.repetitions + 1):
                    print(
                        f"running {dataset} limit={limit} {archetype} {transport} "
                        f"rep={repetition}/{args.repetitions}",
                        file=sys.stderr,
                        flush=True,
                    )
                    run = run_once(
                        benchmark_script,
                        args.url,
                        args.timeout,
                        dataset,
                        limit,
                        archetype,
                        transport,
                        providers,
                        args.strict,
                        args.require_partial,
                    )
                    run.update(group)
                    run["repetition"] = repetition
                    runs.append(run)
                    all_runs.append(run)
                summaries.append(aggregate_group(group, runs))

    mark_cross_transport_mismatches(summaries)

    output_dir = args.output_dir
    json_path = output_dir / f"{args.name}.json"
    csv_path = output_dir / f"{args.name}.csv"
    markdown_path = output_dir / f"{args.name}.md"
    write_json(json_path, {"runs": all_runs, "summaries": summaries})
    write_csv(csv_path, summaries)
    write_markdown(markdown_path, summaries)

    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "markdown": str(markdown_path)}))
    failed = [summary for summary in summaries if summary["failedRuns"] > 0 or not summary["crossTransportResultMatch"]]
    return 1 if failed and args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
