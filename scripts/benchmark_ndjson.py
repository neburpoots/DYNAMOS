#!/usr/bin/env python3

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any


DEFAULT_PROVIDERS = ["UVA", "VU"]
RESPONSE_MODE_BATCHED = "batched"
RESPONSE_MODE_CLASSIC_UNARY = "classic-unary"
WORKLOAD_AVERAGE = "average"
WORKLOAD_BULK = "bulk"
DEFAULT_WORKLOAD = WORKLOAD_AVERAGE
JSON_CONTENT_TYPE = "application/json"


def resolve_tables(dataset: str) -> tuple[str, str]:
    if dataset == "original":
        return "Personen", "Aanstellingen"
    return "PersonenLarge", "AanstellingenLarge"


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_average_query(limit: int, dataset: str) -> str:
    personen_table, aanstellingen_table = resolve_tables(dataset)
    return (
        "SELECT p.Geslacht, s.Salschal "
        f"FROM {personen_table} p "
        f"JOIN {aanstellingen_table} s ON p.Unieknr = s.Unieknr "
        f"LIMIT {limit}"
    )


def build_bulk_query(limit: int, dataset: str) -> str:
    personen_table, aanstellingen_table = resolve_tables(dataset)
    return (
        "SELECT p.Unieknr, p.Geslacht, p.Gebdat, s.Salschal, s.Ingdatdv, s.Functcat "
        f"FROM {personen_table} p "
        f"JOIN {aanstellingen_table} s ON p.Unieknr = s.Unieknr "
        f"LIMIT {limit}"
    )


def build_query(limit: int, dataset: str, workload: str) -> str:
    if workload == WORKLOAD_AVERAGE:
        return build_average_query(limit, dataset)
    if workload == WORKLOAD_BULK:
        return build_bulk_query(limit, dataset)
    raise ValueError(f"unsupported workload {workload!r}")


def algorithm_for_workload(workload: str) -> str:
    if workload == WORKLOAD_AVERAGE:
        return "average"
    if workload == WORKLOAD_BULK:
        return "rows"
    raise ValueError(f"unsupported workload {workload!r}")


def supports_response_mode(transport: str, response_mode: str) -> bool:
    return response_mode == RESPONSE_MODE_BATCHED or (
        transport == "unary" and response_mode == RESPONSE_MODE_CLASSIC_UNARY
    )


def uses_ndjson(response_mode: str) -> bool:
    return response_mode != RESPONSE_MODE_CLASSIC_UNARY


def request_headers(response_mode: str) -> dict[str, str]:
    accept = "application/x-ndjson" if uses_ndjson(response_mode) else JSON_CONTENT_TYPE
    return {
        "Content-Type": JSON_CONTENT_TYPE,
        "Accept": accept,
    }


def build_payload(
    limit: int,
    transport: str,
    dataset: str,
    archetype: str,
    providers: list[str],
    response_mode: str,
    workload: str,
) -> dict[str, Any]:
    aggregate = archetype == "dataThroughTtp"
    return {
        "type": "sqlDataRequest",
        "user": {
            "id": "12324",
            "userName": "jorrit.stutterheim@cloudnation.nl",
        },
        "dataProviders": providers,
        "transport": transport,
        "data_request": {
            "type": "sqlDataRequest",
            "query": build_query(limit, dataset, workload),
            "algorithm": algorithm_for_workload(workload),
            "transport": transport,
            "options": {
                "graph": False,
                "aggregate": aggregate,
                "classicUnary": response_mode == RESPONSE_MODE_CLASSIC_UNARY,
            },
        },
    }


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def final_result_hash(final_results: list[dict[str, Any]]) -> str | None:
    return raw_result_hash(final_results)


def result_fingerprint_items(results: list[dict[str, Any]], include_sequence: bool) -> list[dict[str, Any]]:
    normalized = []
    for result in results:
        item = {
            "provider": result.get("provider", ""),
            "result": result.get("result"),
            "resultText": result.get("resultText", ""),
        }
        if include_sequence:
            item["partial"] = bool(result.get("partial", False))
            item["sequence"] = int(result.get("sequence") or 0)
        normalized.append(item)
    return sorted(
        normalized,
        key=lambda item: (
            item.get("provider", ""),
            int(item.get("sequence") or 0),
            canonical_json(item.get("result")) if item.get("result") is not None else "",
            item.get("resultText", ""),
        ),
    )


def raw_result_hash(results: list[dict[str, Any]]) -> str | None:
    if not results:
        return None
    return hashlib.sha256(canonical_json(result_fingerprint_items(results, True)).encode("utf-8")).hexdigest()


def table_rows(result: Any) -> list[dict[str, Any]] | None:
    if not isinstance(result, list):
        return None
    if any(not isinstance(item, list) for item in result):
        return None
    if not result:
        return []

    header = [str(item) for item in result[0]]
    rows = []
    for row in result[1:]:
        rows.append(
            {
                column: row[index] if index < len(row) else ""
                for index, column in enumerate(header)
            }
        )
    return rows


def content_result_hash(results: list[dict[str, Any]]) -> str | None:
    if not results:
        return None

    tables_by_provider: dict[str, list[dict[str, Any]]] = {}
    non_table_results: list[dict[str, Any]] = []
    for result in results:
        rows = table_rows(result.get("result"))
        if rows is None:
            non_table_results.append(result)
            continue
        tables_by_provider.setdefault(result.get("provider", ""), []).extend(rows)

    normalized = {
        "tables": [
            {
                "provider": provider,
                "rows": sorted(rows, key=canonical_json),
            }
            for provider, rows in sorted(tables_by_provider.items())
        ],
        "results": result_fingerprint_items(non_table_results, False),
    }
    return hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()


def result_hashes(provider_results: list[dict[str, Any]], final_results: list[dict[str, Any]], workload: str) -> dict[str, str | None]:
    content_results = provider_results if workload == WORKLOAD_BULK else final_results
    return {
        "contentResultHash": content_result_hash(content_results),
        "rawResultHash": raw_result_hash(content_results),
        "finalResultHash": content_result_hash(content_results),
    }


def result_from_response_item(item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        stripped = item.strip()
        if not stripped:
            return {
                "provider": "",
                "result": None,
                "resultText": "",
            }
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return {
                "provider": "",
                "result": None,
                "resultText": item,
            }
        return {
            "provider": "",
            "result": parsed,
            "resultText": "",
        }

    return {
        "provider": "",
        "result": item,
        "resultText": "",
    }


def result_row_count(result: Any) -> int | None:
    rows = table_rows(result)
    if rows is None:
        return None
    return len(rows)


def result_events_row_count(results: list[dict[str, Any]]) -> int | None:
    if not results:
        return None
    row_counts = []
    for result in results:
        row_count = result_row_count(result.get("result"))
        if row_count is None:
            return None
        row_counts.append(row_count)
    return sum(row_counts)


def parse_non_ndjson_response(body: str) -> dict[str, Any]:
    stripped = body.strip()
    if not stripped:
        return {"error": "empty response body"}

    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        return {"error": f"response was not valid JSON: {exc}"}

    provider_error_count = 0
    final_results: list[dict[str, Any]] = []
    row_counts: list[int] = []
    if isinstance(payload, dict) and isinstance(payload.get("responses"), list):
        provider_error_count += len(payload.get("providerErrors") or [])
        for response in payload["responses"]:
            parsed_response = result_from_response_item(response)
            if parsed_response["result"] is None and parsed_response["resultText"] == "":
                provider_error_count += 1
                continue
            final_results.append(parsed_response)
            row_count = result_row_count(parsed_response["result"])
            if row_count is not None:
                row_counts.append(row_count)
    else:
        parsed_response = result_from_response_item(payload)
        if parsed_response["result"] is None and parsed_response["resultText"] == "":
            provider_error_count = 1
        else:
            final_results.append(parsed_response)
            row_count = result_row_count(parsed_response["result"])
            if row_count is not None:
                row_counts.append(row_count)

    if not final_results:
        return {
            "error": "response did not contain a final result body",
            "providerErrorCount": provider_error_count,
        }

    return {
        "events": ["response", "done"],
        "finalResults": final_results,
        "providerResults": final_results,
        "providerErrorCount": provider_error_count,
        "rowsObserved": sum(row_counts) if len(row_counts) == len(final_results) else None,
    }


def observed_rows(provider_rows: dict[str, dict[str, int]], fallback: int | None) -> int | None:
    if not provider_rows:
        return fallback
    if len(provider_rows) == 1:
        return next(iter(provider_rows.values())).get("rowsProcessed") or fallback
    return sum(row.get("rowsProcessed", 0) for row in provider_rows.values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one DYNAMOS NDJSON benchmark request.")
    parser.add_argument("--url", default="http://127.0.0.1:18080/api/v1/requestApproval")
    parser.add_argument("--transport", required=True, choices=["unary", "streaming", "rabbitmq-streams"])
    parser.add_argument("--response-mode", default=RESPONSE_MODE_BATCHED, choices=[RESPONSE_MODE_BATCHED, RESPONSE_MODE_CLASSIC_UNARY])
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--dataset", choices=["large", "original"], default="large")
    parser.add_argument("--archetype", choices=["dataThroughTtp", "computeToData"], default="dataThroughTtp")
    parser.add_argument("--workload", choices=[WORKLOAD_AVERAGE, WORKLOAD_BULK], default=DEFAULT_WORKLOAD)
    parser.add_argument("--providers", default=",".join(DEFAULT_PROVIDERS))
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--require-partial", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    providers = parse_csv(args.providers)
    if not providers:
        print(json.dumps({"error": "--providers must contain at least one provider"}))
        return 2

    if not supports_response_mode(args.transport, args.response_mode):
        print(
            json.dumps(
                {
                    "ok": False,
                    "transport": args.transport,
                    "responseMode": args.response_mode,
                    "workload": args.workload,
                    "error": "classic-unary response mode is only supported with unary transport",
                }
            )
        )
        return 2
    if args.response_mode == RESPONSE_MODE_CLASSIC_UNARY and args.require_partial:
        print(
            json.dumps(
                {
                    "ok": False,
                    "transport": args.transport,
                    "responseMode": args.response_mode,
                    "workload": args.workload,
                    "error": "--require-partial cannot be used with classic-unary response mode",
                }
            )
        )
        return 2

    payload = build_payload(
        args.limit,
        args.transport,
        args.dataset,
        args.archetype,
        providers,
        args.response_mode,
        args.workload,
    )
    request = urllib.request.Request(
        args.url,
        data=json.dumps(payload).encode("utf-8"),
        headers=request_headers(args.response_mode),
        method="POST",
    )

    started = time.monotonic()
    first_result_seconds = None
    done_seconds = None
    rows_processed = None
    rows_total = None
    events = []
    raw_lines = []
    provider_rows: dict[str, dict[str, int]] = {}
    provider_results = []
    final_results = []
    sequence_by_provider: dict[str, int] = {}
    sequence_errors = []
    partial_result_count = 0
    provider_error_count = 0
    errors = []

    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            content_type = response.headers.get("Content-Type", "")
            if uses_ndjson(args.response_mode) and "application/x-ndjson" not in content_type.lower():
                body = response.read().decode("utf-8", errors="replace")
                print(
                    json.dumps(
                        {
                            "dataset": args.dataset,
                            "archetype": args.archetype,
                            "transport": args.transport,
                            "responseMode": args.response_mode,
                            "workload": args.workload,
                            "limit": args.limit,
                            "error": "response was not NDJSON",
                            "contentType": content_type,
                            "body": body,
                        }
                    )
                )
                return 1

            if not uses_ndjson(args.response_mode):
                body = response.read().decode("utf-8", errors="replace")
                done_seconds = time.monotonic() - started
                parsed_response = parse_non_ndjson_response(body)
                if parsed_response.get("error"):
                    print(
                        json.dumps(
                            {
                                "ok": False,
                                "dataset": args.dataset,
                                "archetype": args.archetype,
                                "transport": args.transport,
                                "responseMode": args.response_mode,
                                "workload": args.workload,
                                "limit": args.limit,
                                "contentType": content_type,
                                "error": parsed_response["error"],
                                "body": body,
                            }
                        )
                    )
                    return 1

                final_results = parsed_response["finalResults"]
                provider_results = parsed_response["providerResults"]
                provider_error_count = parsed_response["providerErrorCount"]
                events = parsed_response["events"]
                raw_lines = [body]
                if final_results:
                    first_result_seconds = done_seconds
                rows_observed = parsed_response.get("rowsObserved")
                if rows_observed is None:
                    rows_observed = observed_rows(provider_rows, rows_processed)
                ok = True
                if args.strict and done_seconds is None:
                    ok = False
                    errors.append("missing done response")
                if args.strict and first_result_seconds is None:
                    ok = False
                    errors.append("missing final response")
                if args.strict and provider_error_count > 0:
                    ok = False
                    errors.append("provider response missing final body")
                if args.strict and sequence_errors:
                    ok = False
                    errors.append("non-monotonic providerResult sequence")
                if args.require_partial and partial_result_count == 0:
                    ok = False
                    errors.append("missing partial providerResult before final")
                if args.expected_rows is not None and rows_observed != args.expected_rows:
                    ok = False
                    errors.append(f"rows mismatch: observed {rows_observed}, expected {args.expected_rows}")

                hashes = result_hashes(provider_results, final_results, args.workload)
                print(
                    json.dumps(
                        {
                            "ok": ok,
                            "dataset": args.dataset,
                            "archetype": args.archetype,
                            "transport": args.transport,
                            "responseMode": args.response_mode,
                            "workload": args.workload,
                            "limit": args.limit,
                            "providers": providers,
                            "expectedRows": args.expected_rows,
                            "firstResultSeconds": first_result_seconds,
                            "doneSeconds": done_seconds,
                            "rowsProcessed": rows_processed,
                            "rowsTotal": rows_total,
                            "observedRows": rows_observed,
                            "providerRows": provider_rows,
                            "partialResultCount": partial_result_count,
                            "providerResultCount": len(provider_results),
                            "finalResultCount": len(final_results),
                            "providerErrorCount": provider_error_count,
                            "sequenceErrors": sequence_errors,
                            **hashes,
                            "events": events,
                            "rawLineCount": len(raw_lines),
                            "errors": errors,
                        }
                    )
                )
                return 0 if ok else 1

            for raw_line in response:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                raw_lines.append(line)
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(
                        json.dumps(
                            {
                                "dataset": args.dataset,
                                "archetype": args.archetype,
                                "transport": args.transport,
                                "responseMode": args.response_mode,
                                "workload": args.workload,
                                "limit": args.limit,
                                "error": f"invalid NDJSON line: {exc}",
                                "rawLine": line,
                            }
                        )
                    )
                    return 1

                event_type = event.get("type", "")
                events.append(event_type)
                elapsed = time.monotonic() - started
                if event_type == "providerError":
                    provider_error_count += 1
                    errors.append(event.get("error", "providerError"))

                if event_type == "providerResult":
                    if first_result_seconds is None:
                        first_result_seconds = elapsed

                    provider = event.get("provider", "")
                    sequence = int(event.get("sequence") or 0)
                    if provider and sequence > 0:
                        previous_sequence = sequence_by_provider.get(provider, 0)
                        if sequence <= previous_sequence:
                            sequence_errors.append(
                                {
                                    "provider": provider,
                                    "previous": previous_sequence,
                                    "current": sequence,
                                }
                            )
                        sequence_by_provider[provider] = sequence

                    rows_processed = event.get("rowsProcessed", rows_processed)
                    rows_total = event.get("rowsTotal", rows_total)
                    if provider:
                        provider_rows[provider] = {
                            "rowsProcessed": int(event.get("rowsProcessed") or 0),
                            "rowsTotal": int(event.get("rowsTotal") or 0),
                        }

                    if event.get("partial", False):
                        partial_result_count += 1
                    else:
                        final_results.append(event)
                    provider_results.append(event)

                if event_type == "done":
                    done_seconds = elapsed
                    break

        rows_observed = observed_rows(provider_rows, rows_processed)
        result_rows_observed = result_events_row_count(provider_results if args.workload == WORKLOAD_BULK else final_results)
        if result_rows_observed is not None:
            rows_observed = result_rows_observed
        ok = True
        if args.strict and done_seconds is None:
            ok = False
            errors.append("missing done event")
        if args.strict and first_result_seconds is None:
            ok = False
            errors.append("missing providerResult event")
        if args.strict and provider_error_count > 0:
            ok = False
        if args.strict and sequence_errors:
            ok = False
            errors.append("non-monotonic providerResult sequence")
        if args.require_partial and partial_result_count == 0:
            ok = False
            errors.append("missing partial providerResult before final")
        if args.expected_rows is not None and rows_observed != args.expected_rows:
            ok = False
            errors.append(f"rows mismatch: observed {rows_observed}, expected {args.expected_rows}")

        hashes = result_hashes(provider_results, final_results, args.workload)
        print(
            json.dumps(
                {
                    "ok": ok,
                    "dataset": args.dataset,
                    "archetype": args.archetype,
                    "transport": args.transport,
                    "responseMode": args.response_mode,
                    "workload": args.workload,
                    "limit": args.limit,
                    "providers": providers,
                    "expectedRows": args.expected_rows,
                    "firstResultSeconds": first_result_seconds,
                    "doneSeconds": done_seconds,
                    "rowsProcessed": rows_processed,
                    "rowsTotal": rows_total,
                    "observedRows": rows_observed,
                    "providerRows": provider_rows,
                    "partialResultCount": partial_result_count,
                    "providerResultCount": len(provider_results),
                    "finalResultCount": len(final_results),
                    "providerErrorCount": provider_error_count,
                    "sequenceErrors": sequence_errors,
                    **hashes,
                    "events": events,
                    "rawLineCount": len(raw_lines),
                    "errors": errors,
                }
            )
        )
        return 0 if ok else 1
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(
            json.dumps(
                {
                    "ok": False,
                    "dataset": args.dataset,
                    "archetype": args.archetype,
                    "transport": args.transport,
                    "responseMode": args.response_mode,
                    "workload": args.workload,
                    "limit": args.limit,
                    "httpStatus": exc.code,
                    "error": body,
                }
            )
        )
        return 1
    except urllib.error.URLError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "dataset": args.dataset,
                    "archetype": args.archetype,
                    "transport": args.transport,
                    "responseMode": args.response_mode,
                    "workload": args.workload,
                    "limit": args.limit,
                    "error": str(exc),
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
