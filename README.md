# sre-log-slo-attribution-analyzer
将高基数日志异常映射到 SLO 消耗和服务责任

## 说明

**输入是离线样本**：本工具不连接任何线上日志平台，只读取本地 JSON
日志样本文件（见 `sample_logs.json`），在离线环境下完成聚合与归因。

## 功能

- 按 `request_id` 聚合唯一失败：重复采集、上游重试、批量任务重复
  上报的日志只计一次，被抑制的重复条数会记录在报告中。
- 上游传播去重：同一请求的错误沿调用链（`caused_by` 标记）传播到
  多个下游服务时，只归因到最上游根因服务，不重复计数。
- 不确定性保留：缺少 `request_id` 的失败进入 `unattributable` 列表，
  按 `(service, template)` 聚类并给出估算权重区间；同一请求采样率
  不一致（采样偏差）时，权重输出 `[low, high]` 区间。
- 预算消耗：错误预算 = `(1 - SLO) × 窗口总请求数`，消耗比例同样以
  区间形式输出。
- 窗口边界：窗口为 `[start, end)` 左闭右开，跨窗口请求按日志时间戳
  归属，窗口外日志单独列出并在 `boundary_note` 中说明。

## 输入格式

每条日志为一个 JSON 对象：

| 字段 | 说明 |
| --- | --- |
| `timestamp` | ISO-8601 时间戳 |
| `service` | 服务名 |
| `template` | 日志模板（如 `http_5xx`） |
| `request_id` | 请求标识，可缺省（`null`） |
| `sample_rate` | 采样比例，`(0, 1]`，`1` 表示全量 |
| `final_result` | 最终结果：`success` / `failure` |
| `caused_by` | 可选，导致本服务失败的上游服务名 |

## 使用

```bash
python slo_attribution.py sample_logs.json \
    --window-start 2026-11-21T00:00:00+00:00 \
    --window-end   2026-11-21T01:00:00+00:00 \
    --slo 0.999 --total-requests 10000
```

输出 JSON 报告，包含：证据日志（`evidence`）、无法归因项
（`unattributable`）和窗口边界说明（`boundary_note`）。

## 测试

```bash
python -m unittest
```

覆盖重复日志去重、上游传播归因、跨窗口请求、缺请求 ID 的不确定性
等场景。代码兼容 Python 3.8.2。
