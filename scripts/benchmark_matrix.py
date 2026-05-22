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


DEFAULT_TRANSPORTS = "unary,streaming,rabbitmq-streams"
DEFAULT_RESPONSE_MODES = "batched,classic-unary"
DEFAULT_DATASETS = "large,original"
DEFAULT_ARCHETYPES = "dataThroughTtp,computeToData"
DEFAULT_WORKLOADS = "bulk,average"
DEFAULT_LARGE_LIMITS = "50000,250000"
DEFAULT_PROVIDER_SETS = "UVA;UVA,VU"
RESPONSE_MODE_BATCHED = "batched"
RESPONSE_MODE_CLASSIC_UNARY = "classic-unary"
WORKLOAD_AVERAGE = "average"
WORKLOAD_BULK = "bulk"
SUPPORTED_RESPONSE_MODES = {RESPONSE_MODE_BATCHED, RESPONSE_MODE_CLASSIC_UNARY}


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_csv(value: str) -> list[int]:
    return [int(item) for item in parse_csv(value)]


def parse_provider_sets(value: str) -> list[list[str]]:
    provider_sets = []
    for item in value.split(";"):
        providers = parse_csv(item)
        if providers:
            provider_sets.append(providers)
    return provider_sets


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


def expected_rows(dataset: str, limit: int, providers: list[str], response_mode: str, workload: str) -> int | None:
    if dataset != "large":
        return None
    if workload == WORKLOAD_AVERAGE and response_mode == RESPONSE_MODE_CLASSIC_UNARY:
        return None
    return limit * len(providers)


def response_modes_for_transport(transport: str, response_modes: list[str]) -> list[str]:
    modes: list[str] = []
    for response_mode in response_modes:
        if response_mode == RESPONSE_MODE_BATCHED:
            modes.append(response_mode)
        elif response_mode == RESPONSE_MODE_CLASSIC_UNARY and transport == "unary":
            modes.append(response_mode)
    return modes


def require_partial_for_case(
    response_mode: str,
    workload: str,
    require_partial: bool,
    expected_row_count: int | None,
    sql_batch_rows: str | None,
) -> bool:
    if not require_partial:
        return False
    if response_mode == RESPONSE_MODE_CLASSIC_UNARY or workload != WORKLOAD_BULK:
        return False
    if expected_row_count is None:
        return False
    try:
        batch_rows = int(sql_batch_rows) if sql_batch_rows else 0
    except ValueError:
        batch_rows = 0
    return batch_rows <= 0 or expected_row_count > batch_rows


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
    response_mode: str,
    workload: str,
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
        "--response-mode",
        response_mode,
        "--workload",
        workload,
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

    rows = expected_rows(dataset, limit, providers, response_mode, workload)
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
    content_hashes = sorted({run.get("contentResultHash") or run.get("finalResultHash") for run in successful if run.get("contentResultHash") or run.get("finalResultHash")})
    raw_hashes = sorted({run.get("rawResultHash") for run in successful if run.get("rawResultHash")})
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
        "contentResultHashes": content_hashes,
        "rawResultHashes": raw_hashes,
        "finalResultHashes": content_hashes,
        "errors": [run.get("errors") or run.get("error") for run in runs if not run.get("ok")],
    }


def mark_cross_transport_mismatches(summaries: list[dict[str, Any]]) -> None:
    content_groups: dict[tuple[Any, ...], set[str]] = {}
    raw_groups: dict[tuple[Any, ...], set[str]] = {}
    for summary in summaries:
        key = (
            summary["dataset"],
            summary["limit"],
            summary["workload"],
            summary["archetype"],
            summary["providersLabel"],
            summary["temperature"],
            summary["responseMode"],
            summary.get("sqlBatchRows"),
            summary.get("rabbitmqChunkRows"),
        )
        content_groups.setdefault(key, set()).update(summary.get("contentResultHashes", []))
        raw_groups.setdefault(key, set()).update(summary.get("rawResultHashes", []))

    for summary in summaries:
        key = (
            summary["dataset"],
            summary["limit"],
            summary["workload"],
            summary["archetype"],
            summary["providersLabel"],
            summary["temperature"],
            summary["responseMode"],
            summary.get("sqlBatchRows"),
            summary.get("rabbitmqChunkRows"),
        )
        summary["crossTransportContentMatch"] = len(content_groups.get(key, set())) <= 1
        summary["crossTransportRawMatch"] = len(raw_groups.get(key, set())) <= 1
        summary["crossTransportResultMatch"] = summary["crossTransportContentMatch"]


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
        "providers",
        "temperature",
        "transport",
        "responseMode",
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
        "crossTransportContentMatch",
        "crossTransportRawMatch",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(
                {
                    "dataset": summary["dataset"],
                    "limit": summary["limit"],
                    "workload": summary["workload"],
                    "archetype": summary["archetype"],
                    "providers": summary["providersLabel"],
                    "temperature": summary["temperature"],
                    "transport": summary["transport"],
                    "responseMode": summary["responseMode"],
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
                    "crossTransportContentMatch": summary["crossTransportContentMatch"],
                    "crossTransportRawMatch": summary["crossTransportRawMatch"],
                }
            )


def write_markdown(path: Path, summaries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |",
        "| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for summary in summaries:
        first = summary["firstResultSeconds"]
        done = summary["doneSeconds"]
        rows = summary["observedRows"]
        lines.append(
            "| {dataset} | {limit} | {workload} | {archetype} | {providers} | {temperature} | {transport} | {response_mode} | {batch} | {chunk} | {ok}/{runs} | "
            "{first_median} ({first_min}-{first_max}) | {done_median} ({done_min}-{done_max}) | {rows_median} | {content_match} | {raw_match} |".format(
                dataset=summary["dataset"],
                limit=summary["limit"],
                workload=summary["workload"],
                archetype=summary["archetype"],
                providers=summary["providersLabel"],
                temperature=summary["temperature"],
                transport=summary["transport"],
                response_mode=summary["responseMode"],
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
                content_match="yes" if summary["crossTransportContentMatch"] else "no",
                raw_match="yes" if summary["crossTransportRawMatch"] else "no",
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated DYNAMOS NDJSON benchmark matrices.")
    parser.add_argument("--url", default="http://127.0.0.1:18080/api/v1/requestApproval")
    parser.add_argument("--benchmark-script", type=Path)
    parser.add_argument("--transports", default=DEFAULT_TRANSPORTS)
    parser.add_argument("--response-modes", default=DEFAULT_RESPONSE_MODES)
    parser.add_argument("--workloads", default=DEFAULT_WORKLOADS)
    parser.add_argument("--datasets", default=DEFAULT_DATASETS)
    parser.add_argument("--large-limits", default=DEFAULT_LARGE_LIMITS)
    parser.add_argument("--original-limit", type=int, default=1_000_000)
    parser.add_argument("--archetypes", default=DEFAULT_ARCHETYPES)
    parser.add_argument("--provider-sets", default=DEFAULT_PROVIDER_SETS)
    parser.add_argument("--providers")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--sql-batch-rows")
    parser.add_argument("--rabbitmq-chunk-rows")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--name", default=time.strftime("matrix-%Y%m%d-%H%M%S"))
    parser.add_argument("--temperature", default="warm")
    parser.add_argument("--shuffle-seed", type=int, default=0)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--require-partial", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    benchmark_script = args.benchmark_script or root / "scripts" / "benchmark_ndjson.py"
    transports = parse_csv(args.transports)
    response_modes = parse_csv(args.response_modes)
    workloads = parse_csv(args.workloads)
    datasets = parse_csv(args.datasets)
    archetypes = parse_csv(args.archetypes)
    if args.providers:
        provider_sets = [parse_csv(args.providers)]
    else:
        provider_sets = parse_provider_sets(args.provider_sets)
    large_limits = parse_int_csv(args.large_limits)
    cases = benchmark_cases(datasets, large_limits, args.original_limit)

    invalid_response_modes = sorted(set(response_modes) - SUPPORTED_RESPONSE_MODES)
    if invalid_response_modes:
        print(json.dumps({"error": f"unsupported response modes: {', '.join(invalid_response_modes)}"}))
        return 2
    if not provider_sets:
        print(json.dumps({"error": "no provider sets selected"}))
        return 2

    groups = []
    for dataset, limit in cases:
        for workload in workloads:
            for providers in provider_sets:
                providers_label = ",".join(providers)
                for archetype in archetypes:
                    for transport in transports:
                        for response_mode in response_modes_for_transport(transport, response_modes):
                            groups.append(
                                {
                                    "dataset": dataset,
                                    "limit": limit,
                                    "workload": workload,
                                    "archetype": archetype,
                                    "providers": providers,
                                    "providersLabel": providers_label,
                                    "temperature": args.temperature,
                                    "transport": transport,
                                    "responseMode": response_mode,
                                    "sqlBatchRows": args.sql_batch_rows,
                                    "rabbitmqChunkRows": args.rabbitmq_chunk_rows,
                                }
                            )

    if not groups:
        print(json.dumps({"error": "no benchmark cases selected after transport/response-mode filtering"}))
        return 2

    all_runs = []
    runs_by_group: dict[tuple[Any, ...], list[dict[str, Any]]] = {
        (
            group["dataset"],
            group["limit"],
            group["workload"],
            group["archetype"],
            group["providersLabel"],
            group["temperature"],
            group["transport"],
            group["responseMode"],
            group["sqlBatchRows"],
            group["rabbitmqChunkRows"],
        ): []
        for group in groups
    }

    for repetition in range(1, args.repetitions + 1):
        repetition_groups = list(groups)
        random.Random(args.shuffle_seed + repetition).shuffle(repetition_groups)
        for group in repetition_groups:
            print(
                f"running {group['dataset']} limit={group['limit']} workload={group['workload']} "
                f"providers={group['providersLabel']} {group['temperature']} {group['archetype']} {group['transport']} "
                f"response-mode={group['responseMode']} rep={repetition}/{args.repetitions}",
                file=sys.stderr,
                flush=True,
            )
            run = run_once(
                benchmark_script,
                args.url,
                args.timeout,
                group["dataset"],
                group["limit"],
                group["archetype"],
                group["transport"],
                group["responseMode"],
                group["workload"],
                group["providers"],
                args.strict,
                require_partial_for_case(
                    group["responseMode"],
                    group["workload"],
                    args.require_partial,
                    expected_rows(
                        group["dataset"],
                        group["limit"],
                        group["providers"],
                        group["responseMode"],
                        group["workload"],
                    ),
                    group["sqlBatchRows"],
                ),
            )
            run.update(group)
            run["repetition"] = repetition
            key = (
                group["dataset"],
                group["limit"],
                group["workload"],
                group["archetype"],
                group["providersLabel"],
                group["temperature"],
                group["transport"],
                group["responseMode"],
                group["sqlBatchRows"],
                group["rabbitmqChunkRows"],
            )
            runs_by_group[key].append(run)
            all_runs.append(run)

    summaries = []
    for group in groups:
        key = (
            group["dataset"],
            group["limit"],
            group["workload"],
            group["archetype"],
            group["providersLabel"],
            group["temperature"],
            group["transport"],
            group["responseMode"],
            group["sqlBatchRows"],
            group["rabbitmqChunkRows"],
        )
        summaries.append(aggregate_group(group, runs_by_group[key]))

    mark_cross_transport_mismatches(summaries)

    output_dir = args.output_dir
    json_path = output_dir / f"{args.name}.json"
    csv_path = output_dir / f"{args.name}.csv"
    markdown_path = output_dir / f"{args.name}.md"
    write_json(json_path, {"runs": all_runs, "summaries": summaries})
    write_csv(csv_path, summaries)
    write_markdown(markdown_path, summaries)

    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "markdown": str(markdown_path)}))
    failed = [summary for summary in summaries if summary["failedRuns"] > 0 or not summary["crossTransportContentMatch"]]
    return 1 if failed and args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
