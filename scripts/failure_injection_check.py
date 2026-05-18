#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def kubectl_json(args: list[str]) -> dict[str, Any]:
    completed = subprocess.run(["kubectl", *args, "-o", "json"], text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return json.loads(completed.stdout)


def find_pod_with_container(namespaces: list[str], container_name: str) -> tuple[str, str] | None:
    for namespace in namespaces:
        pods = kubectl_json(["get", "pods", "-n", namespace])
        for pod in pods.get("items", []):
            phase = pod.get("status", {}).get("phase", "")
            if phase not in {"Pending", "Running"}:
                continue
            spec = pod.get("spec", {})
            containers = spec.get("containers", [])
            if any(container.get("name") == container_name for container in containers):
                return namespace, pod["metadata"]["name"]
    return None


def delete_pod(namespace: str, pod_name: str) -> dict[str, Any]:
    started = time.monotonic()
    completed = subprocess.run(
        ["kubectl", "delete", "pod", pod_name, "-n", namespace, "--wait=false"],
        text=True,
        capture_output=True,
    )
    return {
        "namespace": namespace,
        "pod": pod_name,
        "returnCode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "elapsedSeconds": round(time.monotonic() - started, 3),
    }


def parse_benchmark_output(stdout: str) -> dict[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return {"ok": False, "error": "benchmark produced no output"}
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"invalid benchmark JSON: {exc}", "stdout": stdout}


def run_transport(args: argparse.Namespace, transport: str, benchmark_script: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        str(benchmark_script),
        "--url",
        args.url,
        "--transport",
        transport,
        "--dataset",
        args.dataset,
        "--limit",
        str(args.limit),
        "--archetype",
        args.archetype,
        "--providers",
        args.providers,
        "--timeout",
        str(args.timeout),
        "--strict",
    ]
    if args.dataset == "large":
        expected_rows = args.limit * len(parse_csv(args.providers))
        command.extend(["--expected-rows", str(expected_rows)])

    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(args.kill_delay_seconds)

    kill_result: dict[str, Any]
    target = find_pod_with_container(parse_csv(args.target_namespaces), args.target_container)
    if target is None:
        kill_result = {
            "returnCode": 1,
            "error": f"no pod with container {args.target_container!r} found",
            "targetNamespaces": parse_csv(args.target_namespaces),
        }
    else:
        namespace, pod_name = target
        kill_result = delete_pod(namespace, pod_name)

    stdout, stderr = process.communicate(timeout=args.timeout + 30)
    benchmark_result = parse_benchmark_output(stdout)
    benchmark_result["returnCode"] = process.returncode
    if stderr:
        benchmark_result["stderr"] = stderr.strip()

    return {
        "transport": transport,
        "kill": kill_result,
        "benchmark": benchmark_result,
        "completedAfterKill": bool(benchmark_result.get("ok")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a DYNAMOS request while deleting a dynamic job pod mid-flight.")
    parser.add_argument("--url", default="http://127.0.0.1:18080/api/v1/requestApproval")
    parser.add_argument("--benchmark-script", type=Path)
    parser.add_argument("--transports", default="unary,streaming,rabbitmq-streams")
    parser.add_argument("--dataset", choices=["large", "original"], default="large")
    parser.add_argument("--limit", type=int, default=250000)
    parser.add_argument("--archetype", choices=["dataThroughTtp", "computeToData"], default="dataThroughTtp")
    parser.add_argument("--providers", default="UVA,VU")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--kill-delay-seconds", type=float, default=20.0)
    parser.add_argument("--target-namespaces", default="surf,uva,vu")
    parser.add_argument("--target-container", default="sql-algorithm")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    benchmark_script = args.benchmark_script or root / "scripts" / "benchmark_ndjson.py"
    results = [run_transport(args, transport, benchmark_script) for transport in parse_csv(args.transports)]
    payload = {"results": results}
    output = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    print(output, end="")
    return 0 if all(result["completedAfterKill"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
