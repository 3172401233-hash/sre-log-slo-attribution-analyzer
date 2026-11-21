"""SLO 失败归因聚合器。

将高基数日志样本聚合为"唯一失败"，计算 SLO 错误预算消耗，
并保留因缺少请求 ID 或采样偏差带来的不确定性。

兼容 Python 3.8.2。
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

TERMINAL_STATES = ("success", "failure")


def parse_ts(value: str) -> datetime:
    """解析 ISO-8601 时间戳（允许结尾 Z）。"""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


@dataclass
class LogRecord:
    """一条结构化日志样本。"""

    timestamp: datetime
    service: str
    template: str
    request_id: Optional[str]
    sample_rate: float  # (0, 1]，1 表示全量采集
    final_result: Optional[str]  # success / failure / None(未知)
    message: str = ""
    # 上游传播标记：本服务的失败由该上游服务的错误导致
    caused_by: Optional[str] = None

    @classmethod
    def from_dict(cls, raw: Dict) -> "LogRecord":
        return cls(
            timestamp=parse_ts(raw["timestamp"]),
            service=raw["service"],
            template=raw["template"],
            request_id=raw.get("request_id"),
            sample_rate=float(raw.get("sample_rate", 1.0)),
            final_result=raw.get("final_result"),
            message=raw.get("message", ""),
            caused_by=raw.get("caused_by"),
        )


@dataclass
class UniqueFailure:
    """一次去重后的唯一失败。"""

    request_id: str
    attributed_service: str
    weight: float  # 采样加权后的估算失败次数
    weight_low: float  # 采样率不一致时的不确定区间
    weight_high: float
    evidence: List[LogRecord] = field(default_factory=list)
    suppressed_duplicates: int = 0
    propagated_from: Optional[str] = None  # 若非 None，表示该失败归因于上游


@dataclass
class UnattributableItem:
    """无法唯一归因的失败（缺请求 ID / 结果未知等）。"""

    reason: str
    service: str
    template: str
    estimated_weight_low: float
    estimated_weight_high: float
    evidence: List[LogRecord] = field(default_factory=list)


@dataclass
class WindowReport:
    window_start: datetime
    window_end: datetime
    slo_target: float
    total_requests: float
    unique_failures: List[UniqueFailure]
    unattributable: List[UnattributableItem]
    out_of_window: List[LogRecord]
    boundary_note: str

    @property
    def failure_weight_low(self) -> float:
        return sum(f.weight_low for f in self.unique_failures)

    @property
    def failure_weight_high(self) -> float:
        return sum(f.weight_high for f in self.unique_failures)

    @property
    def budget(self) -> float:
        return (1.0 - self.slo_target) * self.total_requests

    def budget_consumption(self) -> Tuple[float, float]:
        """返回错误预算消耗比例的 (下界, 上界)。"""
        if self.budget <= 0:
            return (0.0, 0.0)
        low = self.failure_weight_low / self.budget
        high = self.failure_weight_high / self.budget
        return (low, high)

    def to_dict(self) -> Dict:
        low, high = self.budget_consumption()
        return {
            "window": {
                "start": self.window_start.isoformat(),
                "end": self.window_end.isoformat(),
                "boundary_note": self.boundary_note,
            },
            "slo_target": self.slo_target,
            "total_requests": self.total_requests,
            "error_budget": self.budget,
            "unique_failure_count": len(self.unique_failures),
            "failure_weight_range": [
                self.failure_weight_low,
                self.failure_weight_high,
            ],
            "budget_consumption_range": [low, high],
            "unique_failures": [
                {
                    "request_id": f.request_id,
                    "attributed_service": f.attributed_service,
                    "weight": f.weight,
                    "weight_range": [f.weight_low, f.weight_high],
                    "suppressed_duplicates": f.suppressed_duplicates,
                    "propagated_from": f.propagated_from,
                    "evidence": [
                        {
                            "timestamp": e.timestamp.isoformat(),
                            "service": e.service,
                            "template": e.template,
                            "sample_rate": e.sample_rate,
                            "message": e.message,
                        }
                        for e in f.evidence
                    ],
                }
                for f in self.unique_failures
            ],
            "unattributable": [
                {
                    "reason": u.reason,
                    "service": u.service,
                    "template": u.template,
                    "estimated_weight_range": [
                        u.estimated_weight_low,
                        u.estimated_weight_high,
                    ],
                    "evidence": [
                        {
                            "timestamp": e.timestamp.isoformat(),
                            "service": e.service,
                            "template": e.template,
                            "message": e.message,
                        }
                        for e in u.evidence
                    ],
                }
                for u in self.unattributable
            ],
            "out_of_window_count": len(self.out_of_window),
        }


def _in_window(ts: datetime, start: datetime, end: datetime) -> bool:
    """窗口为 [start, end) 左闭右开。"""
    return start <= ts < end


def aggregate_failures(
    records: List[LogRecord],
    window_start: datetime,
    window_end: datetime,
    slo_target: float,
    total_requests: float,
) -> WindowReport:
    """聚合窗口内的唯一失败并计算预算消耗。

    规则：
    1. 同一 request_id 的重复日志（重试、重复采集）只计一次。
    2. 同一 request_id 跨多个服务的失败，归因到调用链最上游
       （未被 caused_by 指向的服务），下游传播不重复计数。
    3. 缺 request_id 或最终结果未知的失败进入 unattributable，
       用采样率给出估算区间，保留不确定性。
    4. 同一 request 的样本采样率不一致时，权重给出 [min, max] 区间。
    5. 跨窗口请求按日志时间戳归属窗口；窗口外的日志单独列出。
    """
    in_window: List[LogRecord] = []
    out_of_window: List[LogRecord] = []
    for rec in records:
        if _in_window(rec.timestamp, window_start, window_end):
            in_window.append(rec)
        else:
            out_of_window.append(rec)

    by_request: Dict[str, List[LogRecord]] = defaultdict(list)
    orphans: List[LogRecord] = []
    for rec in in_window:
        if rec.request_id:
            by_request[rec.request_id].append(rec)
        else:
            orphans.append(rec)

    unique_failures: List[UniqueFailure] = []
    unattributable: List[UnattributableItem] = []

    for request_id, group in sorted(by_request.items()):
        failures = [r for r in group if r.final_result == "failure"]
        if not failures:
            continue  # 成功或结果未知的请求不消耗预算

        # 上游传播去重：caused_by 表示"本服务的失败由该上游导致"。
        # 根因是自身没有 caused_by 的失败服务（错误链最上游）；
        # 下游服务的失败日志保留为证据，但不重复计数。
        root_failures = [r for r in failures if not r.caused_by]
        if not root_failures:
            root_failures = failures  # 环状标记时退化为全部
        root = min(root_failures, key=lambda r: r.timestamp)
        propagated_from = None
        downstream = [
            r for r in failures if r is not root and r.service != root.service
        ]
        if downstream:
            propagated_from = root.service

        weights = [1.0 / r.sample_rate for r in root_failures]
        weight_low = min(weights)
        weight_high = max(weights)
        # 去重后的代表权重：取中位数，避免单次异常采样率主导
        weights_sorted = sorted(weights)
        mid = len(weights_sorted) // 2
        if len(weights_sorted) % 2:
            weight = weights_sorted[mid]
        else:
            weight = (weights_sorted[mid - 1] + weights_sorted[mid]) / 2.0

        unique_failures.append(
            UniqueFailure(
                request_id=request_id,
                attributed_service=root.service,
                weight=weight,
                weight_low=weight_low,
                weight_high=weight_high,
                evidence=sorted(group, key=lambda r: r.timestamp),
                suppressed_duplicates=len(group) - 1,
                propagated_from=propagated_from,
            )
        )

    # 缺请求 ID 的失败：按 (service, template) 聚类，保留不确定性
    orphan_groups: Dict[Tuple[str, str], List[LogRecord]] = defaultdict(list)
    for rec in orphans:
        if rec.final_result == "failure":
            orphan_groups[(rec.service, rec.template)].append(rec)
    for (service, template), group in sorted(orphan_groups.items()):
        weights = [1.0 / r.sample_rate for r in group]
        unattributable.append(
            UnattributableItem(
                reason="missing_request_id",
                service=service,
                template=template,
                estimated_weight_low=min(weights),
                estimated_weight_high=sum(weights),
                evidence=sorted(group, key=lambda r: r.timestamp),
            )
        )

    boundary_note = (
        "窗口为 [{start}, {end}) 左闭右开；请求按日志时间戳归属窗口，"
        "跨窗口请求只计入其日志落在窗口内的部分，"
        "窗口外日志共 {n} 条已单独列出。".format(
            start=window_start.isoformat(),
            end=window_end.isoformat(),
            n=len(out_of_window),
        )
    )

    return WindowReport(
        window_start=window_start,
        window_end=window_end,
        slo_target=slo_target,
        total_requests=total_requests,
        unique_failures=unique_failures,
        unattributable=unattributable,
        out_of_window=out_of_window,
        boundary_note=boundary_note,
    )


def load_records(path: str) -> List[LogRecord]:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return [LogRecord.from_dict(item) for item in raw]


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="SLO 失败归因聚合器（离线样本输入）"
    )
    parser.add_argument("logs", help="离线日志样本 JSON 文件")
    parser.add_argument("--window-start", required=True, help="ISO-8601，含起点")
    parser.add_argument("--window-end", required=True, help="ISO-8601，不含终点")
    parser.add_argument("--slo", type=float, required=True, help="SLO 目标，如 0.999")
    parser.add_argument(
        "--total-requests",
        type=float,
        required=True,
        help="窗口内总请求数（采样加权后）",
    )
    args = parser.parse_args(argv)

    records = load_records(args.logs)
    report = aggregate_failures(
        records,
        parse_ts(args.window_start),
        parse_ts(args.window_end),
        args.slo,
        args.total_requests,
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
