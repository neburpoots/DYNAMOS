#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


CPU_SUFFIX_TO_MILLICORES = {
    "n": 1 / 1_000_000,
    "u": 1 / 1_000,
    "m": 1,
}

MEMORY_SUFFIX_TO_BYTES = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
}


def parse_csv(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def parse_cpu_millicores(value: str) -> float:
    for suffix, multiplier in CPU_SUFFIX_TO_MILLICORES.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    return float(value) * 1000


def parse_memory_bytes(value: str) -> float:
    for suffix, multiplier in MEMORY_SUFFIX_TO_BYTES.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    return float(value)


def parse_top_output(
    output: str,
    namespaces: set[str],
    exclude_containers: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        namespace, pod, container, cpu, memory = parts[:5]
        if namespaces and namespace not in namespaces:
            continue
        if container in exclude_containers:
            continue
        try:
            cpu_millicores = parse_cpu_millicores(cpu)
            memory_bytes = parse_memory_bytes(memory)
        except ValueError:
            continue
        rows.append(
            {
                "namespace": namespace,
                "pod": pod,
                "container": container,
                "cpuMillicores": cpu_millicores,
                "memoryBytes": memory_bytes,
            }
        )
    return rows


def sample_kubectl_top(namespaces: set[str], exclude_containers: set[str]) -> tuple[list[dict[str, Any]], str | None]:
    completed = subprocess.run(
        ["kubectl", "top", "pod", "-A", "--containers", "--no-headers"],
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        return [], completed.stderr.strip() or completed.stdout.strip()
    return parse_top_output(completed.stdout, namespaces, exclude_containers), None


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    namespace_max: dict[str, dict[str, float]] = {}
    container_max: dict[str, dict[str, Any]] = {}
    max_total_cpu = 0.0
    max_total_memory = 0.0

    for sample in samples:
        rows = sample["containers"]
        total_cpu = sum(row["cpuMillicores"] for row in rows)
        total_memory = sum(row["memoryBytes"] for row in rows)
        max_total_cpu = max(max_total_cpu, total_cpu)
        max_total_memory = max(max_total_memory, total_memory)

        namespace_totals: dict[str, dict[str, float]] = {}
        for row in rows:
            namespace = row["namespace"]
            totals = namespace_totals.setdefault(namespace, {"cpuMillicores": 0.0, "memoryBytes": 0.0})
            totals["cpuMillicores"] += row["cpuMillicores"]
            totals["memoryBytes"] += row["memoryBytes"]

            key = f"{row['namespace']}/{row['pod']}/{row['container']}"
            current = container_max.setdefault(
                key,
                {
                    "namespace": row["namespace"],
                    "pod": row["pod"],
                    "container": row["container"],
                    "maxCpuMillicores": 0.0,
                    "maxMemoryBytes": 0.0,
                },
            )
            current["maxCpuMillicores"] = max(current["maxCpuMillicores"], row["cpuMillicores"])
            current["maxMemoryBytes"] = max(current["maxMemoryBytes"], row["memoryBytes"])

        for namespace, totals in namespace_totals.items():
            current = namespace_max.setdefault(namespace, {"maxCpuMillicores": 0.0, "maxMemoryBytes": 0.0})
            current["maxCpuMillicores"] = max(current["maxCpuMillicores"], totals["cpuMillicores"])
            current["maxMemoryBytes"] = max(current["maxMemoryBytes"], totals["memoryBytes"])

    return {
        "sampleCount": len(samples),
        "maxTotalCpuMillicores": round(max_total_cpu, 3),
        "maxTotalMemoryBytes": int(max_total_memory),
        "namespaceMax": {
            namespace: {
                "maxCpuMillicores": round(values["maxCpuMillicores"], 3),
                "maxMemoryBytes": int(values["maxMemoryBytes"]),
            }
            for namespace, values in sorted(namespace_max.items())
        },
        "containerMax": sorted(
            (
                {
                    **values,
                    "maxCpuMillicores": round(values["maxCpuMillicores"], 3),
                    "maxMemoryBytes": int(values["maxMemoryBytes"]),
                }
                for values in container_max.values()
            ),
            key=lambda item: (item["namespace"], item["pod"], item["container"]),
        ),
    }


def parse_last_json(stdout: str) -> Any:
    for line in reversed([line.strip() for line in stdout.splitlines() if line.strip()]):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a command while sampling Kubernetes container CPU and memory with kubectl top."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--namespaces", default="api-gateway,orchestrator,uva,surf,vu,core")
    parser.add_argument("--exclude-containers", default="POD")
    parser.add_argument("--keep-samples", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        print("missing command to run", file=sys.stderr)
        return 2

    namespaces = parse_csv(args.namespaces)
    exclude_containers = parse_csv(args.exclude_containers)
    samples: list[dict[str, Any]] = []
    sampling_errors: list[str] = []

    started = time.monotonic()
    started_epoch = time.time()
    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        rows, error = sample_kubectl_top(namespaces, exclude_containers)
        if error:
            sampling_errors.append(error)
        else:
            samples.append({"timestamp": time.time(), "containers": rows})
        time.sleep(args.interval)

    stdout, stderr = process.communicate()
    rows, error = sample_kubectl_top(namespaces, exclude_containers)
    if error:
        sampling_errors.append(error)
    else:
        samples.append({"timestamp": time.time(), "containers": rows})

    wall_seconds = time.monotonic() - started
    payload: dict[str, Any] = {
        "command": command,
        "returnCode": process.returncode,
        "startedEpochSeconds": started_epoch,
        "endedEpochSeconds": time.time(),
        "wallSeconds": round(wall_seconds, 3),
        "samplingIntervalSeconds": args.interval,
        "namespaces": sorted(namespaces),
        "excludeContainers": sorted(exclude_containers),
        "resourceSummary": summarize_samples(samples),
        "samplingErrors": sampling_errors,
        "benchmarkResult": parse_last_json(stdout),
        "stdout": stdout,
        "stderr": stderr,
    }
    if args.keep_samples:
        payload["samples"] = samples

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if stdout:
        print(stdout, end="")
    if stderr:
        print(stderr, end="", file=sys.stderr)
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
