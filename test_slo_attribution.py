# -*- coding: utf-8 -*-
"""slo_attribution 的单元测试（兼容 Python 3.8.2）。"""

import unittest
from datetime import datetime, timedelta, timezone

from slo_attribution import LogRecord, aggregate_failures

UTC = timezone.utc
WINDOW_START = datetime(2026, 11, 21, 0, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 11, 21, 1, 0, tzinfo=UTC)


def make_record(
    minutes=0,
    service="api",
    template="http_5xx",
    request_id="req-1",
    sample_rate=1.0,
    final_result="failure",
    caused_by=None,
    message="",
):
    return LogRecord(
        timestamp=WINDOW_START + timedelta(minutes=minutes),
        service=service,
        template=template,
        request_id=request_id,
        sample_rate=sample_rate,
        final_result=final_result,
        caused_by=caused_by,
        message=message or template,
    )


def run(records, slo=0.999, total=10000.0):
    return aggregate_failures(records, WINDOW_START, WINDOW_END, slo, total)


class DuplicateLogsTest(unittest.TestCase):
    """重复采集 / 上游重试产生的重复日志只计一次。"""

    def test_duplicate_collection_counted_once(self):
        records = [
            make_record(minutes=1, request_id="req-dup"),
            make_record(minutes=1, request_id="req-dup"),
            make_record(minutes=2, request_id="req-dup"),
        ]
        report = run(records)
        self.assertEqual(len(report.unique_failures), 1)
        failure = report.unique_failures[0]
        self.assertEqual(failure.request_id, "req-dup")
        self.assertEqual(failure.weight, 1.0)
        self.assertEqual(failure.suppressed_duplicates, 2)
        self.assertEqual(len(failure.evidence), 3)

    def test_success_request_not_counted(self):
        records = [make_record(request_id="req-ok", final_result="success")]
        report = run(records)
        self.assertEqual(report.unique_failures, [])
        self.assertEqual(report.budget_consumption(), (0.0, 0.0))

    def test_sampling_weight_and_bias_range(self):
        records = [
            make_record(minutes=1, request_id="req-s", sample_rate=0.5),
            make_record(minutes=2, request_id="req-s", sample_rate=0.1),
        ]
        report = run(records)
        failure = report.unique_failures[0]
        self.assertEqual(failure.weight_low, 2.0)
        self.assertEqual(failure.weight_high, 10.0)
        self.assertEqual(failure.weight, 6.0)


class UpstreamPropagationTest(unittest.TestCase):
    """上游错误传播到多个下游时不重复归因。"""

    def test_propagation_attributed_to_root_only(self):
        records = [
            make_record(minutes=1, service="db", request_id="req-p"),
            make_record(
                minutes=2, service="auth", request_id="req-p", caused_by="db"
            ),
            make_record(
                minutes=3, service="gateway", request_id="req-p", caused_by="auth"
            ),
        ]
        report = run(records)
        self.assertEqual(len(report.unique_failures), 1)
        failure = report.unique_failures[0]
        self.assertEqual(failure.attributed_service, "db")
        self.assertEqual(failure.weight, 1.0)
        self.assertEqual(failure.propagated_from, "db")
        services = {e.service for e in failure.evidence}
        self.assertEqual(services, {"db", "auth", "gateway"})

    def test_independent_failures_counted_separately(self):
        records = [
            make_record(minutes=1, service="db", request_id="req-a"),
            make_record(minutes=2, service="auth", request_id="req-b"),
        ]
        report = run(records)
        self.assertEqual(len(report.unique_failures), 2)
        self.assertEqual(report.failure_weight_low, 2.0)


class CrossWindowTest(unittest.TestCase):
    """跨窗口请求按日志时间戳归属，窗口外日志单独列出。"""

    def test_out_of_window_logs_excluded(self):
        records = [
            make_record(minutes=-5, request_id="req-out"),
            make_record(minutes=10, request_id="req-in"),
            make_record(minutes=60, request_id="req-edge"),
        ]
        report = run(records)
        ids = [f.request_id for f in report.unique_failures]
        self.assertEqual(ids, ["req-in"])
        self.assertEqual(len(report.out_of_window), 2)
        self.assertIn("[", report.boundary_note)
        self.assertIn(")", report.boundary_note)

    def test_left_boundary_inclusive(self):
        records = [make_record(minutes=0, request_id="req-edge-in")]
        report = run(records)
        self.assertEqual(len(report.unique_failures), 1)


class UncertaintyTest(unittest.TestCase):
    """缺请求 ID 的失败保留不确定性，不计入唯一失败。"""

    def test_missing_request_id_unattributable(self):
        records = [
            make_record(minutes=1, request_id=None, sample_rate=0.5),
            make_record(minutes=2, request_id=None, sample_rate=0.5),
        ]
        report = run(records)
        self.assertEqual(report.unique_failures, [])
        self.assertEqual(len(report.unattributable), 1)
        item = report.unattributable[0]
        self.assertEqual(item.reason, "missing_request_id")
        self.assertEqual(item.estimated_weight_low, 2.0)
        self.assertEqual(item.estimated_weight_high, 4.0)

    def test_budget_consumption_range(self):
        records = [make_record(minutes=1, request_id="req-x")]
        report = run(records, slo=0.999, total=10000.0)
        low, high = report.budget_consumption()
        self.assertAlmostEqual(low, 0.1)
        self.assertAlmostEqual(high, 0.1)


class ReportShapeTest(unittest.TestCase):
    def test_to_dict_contains_required_sections(self):
        records = [
            make_record(minutes=1, request_id="req-1"),
            make_record(minutes=2, request_id=None),
        ]
        report = run(records)
        data = report.to_dict()
        self.assertIn("window", data)
        self.assertIn("boundary_note", data["window"])
        self.assertEqual(data["unique_failure_count"], 1)
        self.assertEqual(len(data["unique_failures"][0]["evidence"]), 1)
        self.assertEqual(len(data["unattributable"]), 1)
        self.assertIn("budget_consumption_range", data)


if __name__ == "__main__":
    unittest.main()
