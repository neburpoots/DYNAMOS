#!/usr/bin/env python3

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_URL = "http://127.0.0.1:18080/api/v1/requestApproval"
RESPONSE_MODE_CLASSIC_UNARY = "classic-unary"
WORKLOAD_BULK = "bulk"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure the main-branch classic DYNAMOS bulk SQL response path."
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--providers", default="UVA")
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--dataset", default="large")
    parser.add_argument("--archetype", default="dataThroughTtp")
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--output", help="Optional JSON summary path")
    return parser.parse_args()


def build_payload(limit: int, providers: list[str]) -> dict[str, Any]:
    return {
        "type": "sqlDataRequest",
        "user": {"id": "12324", "userName": "jorrit.stutterheim@cloudnation.nl"},
        "dataProviders": providers,
        "data_request": {
            "type": "sqlDataRequest",
            "query": (
                "SELECT p.Unieknr, p.Geslacht, p.Gebdat, s.Salschal, s.Ingdatdv, s.Functcat "
                "FROM PersonenLarge p JOIN AanstellingenLarge s ON p.Unieknr = s.Unieknr "
                f"LIMIT {limit}"
            ),
            # Any non-average algorithm keeps the main branch on its full row-table path.
            "algorithm": "rows",
            "options": {"graph": False, "aggregate": True},
            "requestMetadata": {},
        },
    }


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def table_rows(result: Any) -> list[dict[str, Any]] | None:
    if not isinstance(result, list):
        return None
    if any(not isinstance(item, list) for item in result):
        return None
    if not result:
        return []

    header = [str(item) for item in result[0]]
    return [
        {
            column: row[index] if index < len(row) else ""
            for index, column in enumerate(header)
        }
        for row in result[1:]
    ]


def content_result_hash(parsed_results: list[Any]) -> str | None:
    if not parsed_results:
        return None

    table_rows_all: list[dict[str, Any]] = []
    non_table_results: list[Any] = []
    for result in parsed_results:
        rows = table_rows(result)
        if rows is None:
            non_table_results.append(result)
        else:
            table_rows_all.extend(rows)

    normalized = {
        "tables": sorted(table_rows_all, key=canonical_json),
        "results": non_table_results,
    }
    return hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()


def raw_result_hash(raw_results: list[str]) -> str | None:
    if not raw_results:
        return None
    return hashlib.sha256(canonical_json(raw_results).encode("utf-8")).hexdigest()


def parse_provider_result(raw_result: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {
        "bytes": len(raw_result.encode("utf-8")),
        "sha256": hashlib.sha256(raw_result.encode("utf-8")).hexdigest(),
        "empty": raw_result == "",
    }
    if raw_result == "":
        return parsed

    try:
        result_json = json.loads(raw_result)
    except json.JSONDecodeError as exc:
        parsed["parseError"] = str(exc)
        return parsed

    parsed["_parsedResult"] = result_json
    parsed["jsonType"] = type(result_json).__name__
    if isinstance(result_json, list) and result_json and isinstance(result_json[0], list):
        parsed["columns"] = result_json[0]
        parsed["observedRows"] = max(0, len(result_json) - 1)
    return parsed


def parse_gateway_response(body: bytes) -> dict[str, Any]:
    parsed: dict[str, Any] = {
        "responseBytes": len(body),
        "responseSha256": hashlib.sha256(body).hexdigest(),
    }
    if not body:
        parsed["gatewayError"] = "empty response body"
        return parsed

    try:
        response_json = json.loads(body)
    except json.JSONDecodeError as exc:
        parsed["gatewayError"] = f"invalid JSON: {exc}"
        parsed["bodyPreview"] = body[:500].decode("utf-8", errors="replace")
        return parsed

    parsed["jobId"] = response_json.get("jobId")
    responses = response_json.get("responses")
    if not isinstance(responses, list):
        parsed["gatewayError"] = "response JSON does not contain a responses list"
        return parsed

    provider_results = [
        parse_provider_result(response) if isinstance(response, str) else {"type": type(response).__name__}
        for response in responses
    ]
    parsed_results = [
        result["_parsedResult"]
        for result in provider_results
        if isinstance(result, dict) and "_parsedResult" in result
    ]
    raw_results = [response for response in responses if isinstance(response, str)]
    for result in provider_results:
        if isinstance(result, dict):
            result.pop("_parsedResult", None)
    parsed["providerResults"] = provider_results
    parsed["observedRows"] = sum(
        result.get("observedRows", 0)
        for result in parsed["providerResults"]
        if isinstance(result, dict)
    )
    parsed["contentResultHash"] = content_result_hash(parsed_results)
    parsed["rawResultHash"] = raw_result_hash(raw_results)
    parsed["finalResultHash"] = parsed["contentResultHash"]
    parsed["providerResultCount"] = len(parsed_results)
    parsed["finalResultCount"] = len(parsed_results)
    parsed["providerErrorCount"] = len([result for result in provider_results if result.get("empty")])
    return parsed


def run(args: argparse.Namespace) -> dict[str, Any]:
    providers = [provider.strip().upper() for provider in args.providers.split(",") if provider.strip()]
    if args.limit <= 0:
        raise SystemExit("--limit must be greater than zero")
    if not providers:
        raise SystemExit("--providers must contain at least one provider")

    payload = build_payload(args.limit, providers)
    request_body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        args.url,
        data=request_body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    started = time.perf_counter()
    status = None
    response_body = b""
    error = None
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            status = response.status
            response_body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        response_body = exc.read()
        error = str(exc)
    except Exception as exc:  # pylint: disable=broad-except
        error = f"{type(exc).__name__}: {exc}"

    summary: dict[str, Any] = {
        "dataset": args.dataset,
        "archetype": args.archetype,
        "transport": "unary",
        "responseMode": RESPONSE_MODE_CLASSIC_UNARY,
        "workload": WORKLOAD_BULK,
        "url": args.url,
        "limit": args.limit,
        "providers": providers,
        "expectedRows": args.expected_rows,
        "clientTimeoutSeconds": args.timeout,
        "requestBytes": len(request_body),
        "status": status,
        "elapsedSeconds": round(time.perf_counter() - started, 6),
    }
    if error:
        summary["error"] = error
    if response_body:
        summary.update(parse_gateway_response(response_body))
    summary["firstResultSeconds"] = summary["elapsedSeconds"] if status == 200 else None
    summary["doneSeconds"] = summary["elapsedSeconds"] if status == 200 else None
    summary["partialResultCount"] = 0
    summary["events"] = ["response", "done"] if status == 200 else []
    summary["rawLineCount"] = 1 if response_body else 0

    errors = []
    if error:
        errors.append(error)
    if summary.get("gatewayError"):
        errors.append(summary["gatewayError"])
    if args.strict and status != 200:
        errors.append(f"HTTP status {status}")
    if args.strict and summary.get("providerErrorCount", 0) > 0:
        errors.append("provider response missing final body")
    if args.expected_rows is not None and summary.get("observedRows") != args.expected_rows:
        errors.append(f"rows mismatch: observed {summary.get('observedRows')}, expected {args.expected_rows}")
    summary["errors"] = errors
    summary["ok"] = status == 200 and not errors
    return summary


def main() -> int:
    args = parse_args()
    summary = run(args)
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
