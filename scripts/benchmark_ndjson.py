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


def resolve_tables(dataset: str) -> tuple[str, str]:
    if dataset == "original":
        return "Personen", "Aanstellingen"
    return "PersonenLarge", "AanstellingenLarge"


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_query(limit: int, dataset: str) -> str:
    personen_table, aanstellingen_table = resolve_tables(dataset)
    return (
        "SELECT p.Geslacht, s.Salschal "
        f"FROM {personen_table} p "
        f"JOIN {aanstellingen_table} s ON p.Unieknr = s.Unieknr "
        f"LIMIT {limit}"
    )


def build_payload(limit: int, transport: str, dataset: str, archetype: str, providers: list[str]) -> dict[str, Any]:
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
            "query": build_query(limit, dataset),
            "algorithm": "average",
            "transport": transport,
            "options": {
                "graph": False,
                "aggregate": aggregate,
            },
        },
    }


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def final_result_hash(final_results: list[dict[str, Any]]) -> str | None:
    if not final_results:
        return None
    normalized = [
        {
            "provider": result.get("provider", ""),
            "result": result.get("result"),
            "resultText": result.get("resultText", ""),
        }
        for result in sorted(final_results, key=lambda item: item.get("provider", ""))
    ]
    return hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()


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
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--dataset", choices=["large", "original"], default="large")
    parser.add_argument("--archetype", choices=["dataThroughTtp", "computeToData"], default="dataThroughTtp")
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

    payload = build_payload(args.limit, args.transport, args.dataset, args.archetype, providers)
    request = urllib.request.Request(
        args.url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/x-ndjson",
        },
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
    final_results = []
    sequence_by_provider: dict[str, int] = {}
    sequence_errors = []
    partial_result_count = 0
    provider_error_count = 0
    errors = []

    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            content_type = response.headers.get("Content-Type", "")
            if "application/x-ndjson" not in content_type.lower():
                body = response.read().decode("utf-8", errors="replace")
                print(
                    json.dumps(
                        {
                            "dataset": args.dataset,
                            "archetype": args.archetype,
                            "transport": args.transport,
                            "limit": args.limit,
                            "error": "response was not NDJSON",
                            "contentType": content_type,
                            "body": body,
                        }
                    )
                )
                return 1

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

                if event_type == "done":
                    done_seconds = elapsed
                    break

        rows_observed = observed_rows(provider_rows, rows_processed)
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

        print(
            json.dumps(
                {
                    "ok": ok,
                    "dataset": args.dataset,
                    "archetype": args.archetype,
                    "transport": args.transport,
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
                    "finalResultCount": len(final_results),
                    "providerErrorCount": provider_error_count,
                    "sequenceErrors": sequence_errors,
                    "finalResultHash": final_result_hash(final_results),
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
                    "limit": args.limit,
                    "error": str(exc),
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
