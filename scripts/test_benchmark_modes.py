#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


benchmark_ndjson = load_module("benchmark_ndjson", ROOT / "scripts" / "benchmark_ndjson.py")
benchmark_matrix = load_module("benchmark_matrix", ROOT / "scripts" / "benchmark_matrix.py")


class BenchmarkModeTests(unittest.TestCase):
    def test_build_payload_marks_classic_unary_requests(self):
        payload = benchmark_ndjson.build_payload(
            10,
            "unary",
            "large",
            "dataThroughTtp",
            ["UVA", "VU"],
            benchmark_ndjson.RESPONSE_MODE_CLASSIC_UNARY,
            benchmark_ndjson.WORKLOAD_AVERAGE,
        )

        self.assertTrue(payload["data_request"]["options"]["classicUnary"])
        self.assertTrue(payload["data_request"]["options"]["aggregate"])
        self.assertEqual(payload["data_request"]["algorithm"], "average")

    def test_build_payload_keeps_batched_requests_disabled(self):
        payload = benchmark_ndjson.build_payload(
            10,
            "unary",
            "large",
            "computeToData",
            ["UVA"],
            benchmark_ndjson.RESPONSE_MODE_BATCHED,
            benchmark_ndjson.WORKLOAD_AVERAGE,
        )

        self.assertFalse(payload["data_request"]["options"]["classicUnary"])
        self.assertFalse(payload["data_request"]["options"]["aggregate"])

    def test_build_payload_uses_fixed_projection_for_bulk(self):
        payload = benchmark_ndjson.build_payload(
            10,
            "unary",
            "large",
            "dataThroughTtp",
            ["UVA"],
            benchmark_ndjson.RESPONSE_MODE_BATCHED,
            benchmark_ndjson.WORKLOAD_BULK,
        )

        self.assertEqual(payload["data_request"]["algorithm"], "rows")
        self.assertIn("SELECT p.Unieknr, p.Geslacht, p.Gebdat", payload["data_request"]["query"])

    def test_request_headers_use_json_for_classic_unary(self):
        headers = benchmark_ndjson.request_headers(benchmark_ndjson.RESPONSE_MODE_CLASSIC_UNARY)

        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["Accept"], "application/json")

    def test_parse_non_ndjson_response_reads_final_responses(self):
        payload = {
            "jobId": "job-1",
            "responses": [
                '{"avg_salary_scale_men":"12.000"}',
                '{"avg_salary_scale_women":"11.000"}',
            ],
        }

        parsed = benchmark_ndjson.parse_non_ndjson_response(benchmark_ndjson.canonical_json(payload))

        self.assertEqual(parsed["events"], ["response", "done"])
        self.assertEqual(parsed["providerErrorCount"], 0)
        self.assertEqual(len(parsed["finalResults"]), 2)
        self.assertIsNone(parsed["rowsObserved"])

    def test_parse_non_ndjson_response_counts_bulk_rows(self):
        payload = {
            "jobId": "job-1",
            "responses": [
                '[["Unieknr","Geslacht"],["1","M"],["2","V"]]',
                '[["Unieknr","Geslacht"],["3","M"]]',
            ],
        }

        parsed = benchmark_ndjson.parse_non_ndjson_response(benchmark_ndjson.canonical_json(payload))

        self.assertEqual(parsed["rowsObserved"], 3)

    def test_bulk_content_hash_ignores_column_and_chunk_order(self):
        first = [
            {
                "provider": "UVA",
                "sequence": 1,
                "partial": True,
                "result": [["Unieknr", "Geslacht"], ["1", "M"]],
                "resultText": "",
            },
            {
                "provider": "UVA",
                "sequence": 2,
                "partial": False,
                "result": [["Unieknr", "Geslacht"], ["2", "V"]],
                "resultText": "",
            },
        ]
        second = [
            {
                "provider": "UVA",
                "sequence": 1,
                "partial": False,
                "result": [["Geslacht", "Unieknr"], ["V", "2"], ["M", "1"]],
                "resultText": "",
            }
        ]

        self.assertEqual(benchmark_ndjson.content_result_hash(first), benchmark_ndjson.content_result_hash(second))
        self.assertNotEqual(benchmark_ndjson.raw_result_hash(first), benchmark_ndjson.raw_result_hash(second))

    def test_result_events_row_count_sums_bulk_chunks(self):
        events = [
            {"result": [["Unieknr"], ["1"], ["2"]]},
            {"result": [["Unieknr"], ["3"]]},
        ]

        self.assertEqual(benchmark_ndjson.result_events_row_count(events), 3)

    def test_response_modes_for_unary_include_classic_unary(self):
        modes = benchmark_matrix.response_modes_for_transport(
            "unary",
            [benchmark_matrix.RESPONSE_MODE_BATCHED, benchmark_matrix.RESPONSE_MODE_CLASSIC_UNARY],
        )

        self.assertEqual(
            modes,
            [benchmark_matrix.RESPONSE_MODE_BATCHED, benchmark_matrix.RESPONSE_MODE_CLASSIC_UNARY],
        )

    def test_response_modes_for_streaming_skip_classic_unary(self):
        modes = benchmark_matrix.response_modes_for_transport(
            "streaming",
            [benchmark_matrix.RESPONSE_MODE_BATCHED, benchmark_matrix.RESPONSE_MODE_CLASSIC_UNARY],
        )

        self.assertEqual(modes, [benchmark_matrix.RESPONSE_MODE_BATCHED])

    def test_require_partial_is_disabled_for_classic_unary(self):
        self.assertTrue(
            benchmark_matrix.require_partial_for_case(
                benchmark_matrix.RESPONSE_MODE_BATCHED,
                benchmark_matrix.WORKLOAD_BULK,
                True,
                10_000,
                "5000",
            )
        )
        self.assertFalse(
            benchmark_matrix.require_partial_for_case(
                benchmark_matrix.RESPONSE_MODE_BATCHED,
                benchmark_matrix.WORKLOAD_AVERAGE,
                True,
                10_000,
                "5000",
            )
        )
        self.assertFalse(
            benchmark_matrix.require_partial_for_case(
                benchmark_matrix.RESPONSE_MODE_BATCHED,
                benchmark_matrix.WORKLOAD_BULK,
                True,
                5_000,
                "5000",
            )
        )
        self.assertFalse(
            benchmark_matrix.require_partial_for_case(
                benchmark_matrix.RESPONSE_MODE_CLASSIC_UNARY,
                benchmark_matrix.WORKLOAD_BULK,
                True,
                10_000,
                "5000",
            )
        )

    def test_parse_provider_sets_supports_single_and_multi_provider_benchmarks(self):
        self.assertEqual(benchmark_matrix.parse_provider_sets("UVA;UVA,VU"), [["UVA"], ["UVA", "VU"]])

    def test_expected_rows_disabled_only_for_average_classic_unary(self):
        self.assertIsNone(
            benchmark_matrix.expected_rows(
                "large",
                10,
                ["UVA", "VU"],
                benchmark_matrix.RESPONSE_MODE_CLASSIC_UNARY,
                benchmark_matrix.WORKLOAD_AVERAGE,
            )
        )
        self.assertEqual(
            benchmark_matrix.expected_rows(
                "large",
                10,
                ["UVA", "VU"],
                benchmark_matrix.RESPONSE_MODE_BATCHED,
                benchmark_matrix.WORKLOAD_AVERAGE,
            ),
            20,
        )
        self.assertEqual(
            benchmark_matrix.expected_rows(
                "large",
                10,
                ["UVA", "VU"],
                benchmark_matrix.RESPONSE_MODE_CLASSIC_UNARY,
                benchmark_matrix.WORKLOAD_BULK,
            ),
            20,
        )


if __name__ == "__main__":
    unittest.main()
