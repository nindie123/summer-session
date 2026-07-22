# Layer 4 接口契约

> 本文档定义 L3 (实时计算层) 向 L4 (LLM Agent 智能诊断层) 提供的数据接口。  
> **当前运行方式：** bridge.py (Python) 从 Kafka 消费体征数据，计算 MEWS 评分后产出诊断数据  
> **原始设计：** Flink (Java) — 代码在 `flink_computation/` 目录下保留完整  
> **交付对象：** L4 开发团队  
> **更新方式：** 如有接口变更，维护团队更新此文并通知 L4

---

## 目录

1. [概述](#一概述)
2. [数据流概览](#二数据流概览)
3. [Kafka 接口](#三kafka-接口)
4. [REST API 接口](#四rest-api-接口)
5. [数据格式详解](#五数据格式详解)
6. [集成指南](#六集成指南)
7. [FAQ](#七faq)

---

## 一、概述

L3 产出以下数据供 L4（LLM Agent）消费：

| 接口类型 | 方式 | 内容 | 实时性 |
|---------|------|------|--------|
| 患者综合状态流 | Kafka | 融合体征 + MEWS + 趋势 + 异常 | 秒级实时 |
| 告警事件流 | Kafka | 异常事件、风险分级 | 秒级实时 |
| 历史数据查询 | REST API | 体征时序、MEWS 历史、告警记录 | 近实时 |
| 患者当前状态快照 | REST API | 患者最新综合状态 | 实时 |
| **历史数据批量读取** ⭐ | **HBase** | **体征+MEWS+趋势综合表** | **近实时** |

### ⭐ 新增：HBase 数据存储

**推荐方式：L4 通过 HBase 读取历史数据，比 REST API 更适合批量分析。**

详见 [hbase-design.md](./hbase-design.md)，包含完整的 Python 读取示例。

```python
import happybase
table = happybase.Connection('localhost', 9090).table('vitals')
for key, data in table.scan(row_prefix=b'P0001_', limit=5):
    print(data)
```

### 1.1 L4 需要提供给 L3 的信息

当前阶段 L3 不需要 L4 提供数据。L3 → L4 是**单向数据流**。

> 未来如果需要（如 L4 诊断结论回写），另行协商。

---

## 二、数据流概览

```
L3 Flink 计算层
    │
    ├── Kafka: ai.diagnostic.input ──────────────────→ L4 (LLM Agent)
    │    每条消息 = 一位患者一个时间窗口的综合状态
    │    Key: patientId, Value: DiagnosticInput JSON
    │
    ├── Kafka: ai.alerts ───────────────────────────→ L4 (LLM Agent)
    │    触发条件：风险等级 ≥ WARNING
    │    去重策略：同患者同 type 30s 静默
    │
    └── REST API (api-gateway:8000) ────────────────→ L4 (LLM Agent)
      可根据 traceId / patientId / 时间范围查询历史
```

---

## 三、Kafka 接口

### 3.1 连接信息

| 项目 | 值（开发环境） |
|------|---------------|
| Bootstrap Servers | `localhost:9092` |
| 安全协议 | PLAINTEXT（开发环境） |
| 序列化 | JSON (UTF-8) |

### 3.2 Topic: `ai.diagnostic.input`

**用途：** L3 实时推送每位患者的综合状态，L4 据此进行 LLM 智能诊断。

| 属性 | 值 |
|------|-----|
| topic | `ai.diagnostic.input` |
| partitions | 4 |
| replication | 1 (开发环境) |
| key | `patientId` (String) |
| value | DiagnosticInput (JSON) |
| 保留策略 | 7 天 |

**建议 L4 Consumer Group ID：** `llm-agent`

#### Value Schema

```json
{
  "schemaVersion": "1.0",
  "messageId": "msg_a1b2c3d4e5f6a7b8c9d0e1f2g3h",
  "traceId": "trace_00112233-4455-6677-8899-aabbccddeeff",

  "patientId": "P0001",
  "timestamp": "2026-07-21T10:00:00.000Z",
  "windowStart": "2026-07-21T09:59:58.000Z",
  "windowEnd": "2026-07-21T10:00:00.000Z",

  "vitals": {
    "heartRate":     {"value": 72,  "unit": "/min", "trend": "stable",   "changeRate": 0.5,  "isAnomalous": false},
    "sysBP":         {"value": 120, "unit": "mmHg", "trend": "stable",   "changeRate": 1.2,  "isAnomalous": false},
    "diaBP":         {"value": 80,  "unit": "mmHg", "trend": "stable",   "changeRate": 0.8,  "isAnomalous": false},
    "spo2":          {"value": 98,  "unit": "%",    "trend": "stable",   "changeRate": -0.1, "isAnomalous": false},
    "respiratoryRate": {"value": 16, "unit": "/min", "trend": "stable",  "changeRate": 0.2,  "isAnomalous": false},
    "temperature":   {"value": 36.8,"unit": "°C",   "trend": "stable",   "changeRate": 0.0,  "isAnomalous": false}
  },

  "mews": {
    "totalScore": 1,
    "components": {
      "heartRate": 0,
      "sysBP": 0,
      "respiratoryRate": 0,
      "temperature": 0,
      "avpu": 0
    },
    "riskLevel": "STABLE"
  },

  "anomalies": [],

  "activeDevices": ["monitor_001", "ventilator_001", "temp_001"],
  "dataQuality": {"overall": "good", "signalLost": false, "artifactsDetected": false}
}
```

#### 字段详解

| 路径 | 类型 | 说明 |
|------|------|------|
| `schemaVersion` | string | Schema 版本号，L4 据此选择解析器 |
| `messageId` | string | 全局唯一消息 ID，用于幂等消费 |
| `traceId` | string | 全链路追踪 ID，L4 处理时延续使用 |
| `timestamp` | string | ISO 8601 UTC，当前窗口的结束时间 |
| `vitals.*.trend` | enum | `stable` 稳定 / `rising` 上升 / `falling` 下降 / `rapid_change` 快速变化 |
| `vitals.*.changeRate` | number | 5 分钟内变化率（单位/分钟），正值上升 |
| `vitals.*.isAnomalous` | bool | 是否偏离基线 > 3σ |
| `mews.totalScore` | int | MEWS 总分（0-14），≥7 为危急 |
| `mews.riskLevel` | enum | `STABLE` / `WARNING` / `CRITICAL` / `EMERGENCY` |
| `anomalies[]` | array | 当前窗口所有异常（可能为空） |
| `dataQuality.signalLost` | bool | 是否有设备信号丢失 |

#### 支持的体征参数

| 参数 key | LOINC | 含义 | 单位 | 正常范围 |
|----------|-------|------|------|---------|
| `heartRate` | 8867-4 | 心率 | /min | 60-100 |
| `sysBP` | 8480-6 | 收缩压 | mmHg | 90-140 |
| `diaBP` | 8462-4 | 舒张压 | mmHg | 60-90 |
| `spo2` | 2708-6 | 血氧饱和度 | % | 95-100 |
| `respiratoryRate` | 9279-1 | 呼吸频率 | /min | 12-20 |
| `temperature` | 8310-5 | 体温 | °C | 36.1-37.5 |

#### 风险等级定义

| 等级 | 含义 | MEWS | L4 响应建议 |
|------|------|------|------------|
| STABLE | 稳定 | 0-4 | 常规监护，无需干预 |
| WARNING | 预警 | 5-6 或趋势异常 | 建议关注，可选分析趋势 |
| CRITICAL | 危急 | 7-8 | 建议立即分析并给出处置建议 |
| EMERGENCY | 濒死 | ≥9 | 紧急处理，建议即刻分析 |

### 3.3 Topic: `ai.alerts`

**用途：** L3 检测到风险事件时推送告警，L4 可据此触发主动诊断或通知。

| 属性 | 值 |
|------|-----|
| topic | `ai.alerts` |
| partitions | 2 |
| key | `patientId` (String) |
| value | AlertEvent (JSON) |
| 保留策略 | 30 天（告警需长期留存） |

#### Value Schema

```json
{
  "alertId": "alert_a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "traceId": "trace_00112233-4455-6677-8899-aabbccddeeff",

  "patientId": "P0001",
  "timestamp": "2026-07-21T10:00:00.000Z",

  "type": "MEWS_THRESHOLD",
  "severity": "CRITICAL",
  "mewsScore": 8,
  "riskLevel": "CRITICAL",

  "trigger": {
    "primary": {
      "parameter": "heartRate",
      "value": 42,
      "threshold": 45,
      "trend": "falling"
    },
    "contributing": [
      {"parameter": "sysBP", "value": 85, "trend": "falling", "deviationPercent": -12}
    ]
  },

  "description": "HR 降至 42/min，低于危急阈值；SBP 同步下降 12%",
  "suggestedAction": "立即检查患者意识状态，准备 CPR",

  "vitalSnapshot": {
    "heartRate": 42,
    "sysBP": 85,
    "diaBP": 55,
    "spo2": 94,
    "respiratoryRate": 10,
    "temperature": 36.5
  },

  "windowStart": "2026-07-21T09:59:50.000Z",
  "windowEnd": "2026-07-21T10:00:00.000Z"
}
```

#### 告警类型枚举

| type | 说明 | 触发条件 | severity 范围 |
|------|------|---------|--------------|
| `MEWS_THRESHOLD` | MEWS 评分超阈值 | MEWS ≥ 5 | WARNING / CRITICAL / EMERGENCY |
| `TREND_ABNORMAL` | 趋势异常 | 任一参数 5min 变化率 > 15% | WARNING |
| `BASELINE_DEVIATION` | 基线偏离 | 参数偏离患者自身基线 > 3σ | WARNING / CRITICAL |
| `SIGNAL_LOSS` | 信号丢失 | 设备数据中断超过预期时间 2 倍 | WARNING |
| `CROSS_PARAMETER` | 跨参数异常 | 多参数联合异常模式 | CRITICAL / EMERGENCY |

#### 告警去重规则

```
同 patientId + 同 type + 同 trigger.parameter
→ 30 秒内不重复推送
→ 30 秒后如果该状态仍未恢复，重新推送（含 updated: true）
```

---

## 四、REST API 接口

### 4.1 Base URL

```
开发环境: http://localhost:8000/api/v1
生产环境: (部署后提供)
```

### 4.2 接口列表

#### GET /api/v1/patients/{patientId}/vitals

获取患者时序体征数据。

**参数：**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| start | string(ISO8601) | 否 | 24h 前 | 起始时间 |
| end | string(ISO8601) | 否 | 当前 | 结束时间 |
| limit | int | 否 | 1000 | 返回条数上限 |
| parameters | string | 否 | 全部 | 逗号分隔，如 `heartRate,spo2` |

**响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "patientId": "P0001",
    "parameters": ["heartRate", "spo2"],
    "points": [
      {"timestamp": "2026-07-21T09:00:00.000Z", "heartRate": 72, "spo2": 98},
      {"timestamp": "2026-07-21T09:00:01.000Z", "heartRate": 73, "spo2": 98}
    ]
  },
  "pagination": {"page": 1, "pageSize": 1000, "total": 2},
  "traceId": "trace_..."
}
```

#### GET /api/v1/patients/{patientId}/mews

获取患者 MEWS 评分历史。

**参数：** 同 `/vitals`（start, end, limit）

**响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "patientId": "P0001",
    "points": [
      {
        "timestamp": "2026-07-21T09:00:00.000Z",
        "totalScore": 1,
        "riskLevel": "STABLE",
        "components": {"heartRate": 0, "sysBP": 0, "respiratoryRate": 0, "temperature": 0}
      }
    ]
  },
  "pagination": {"page": 1, "pageSize": 1000, "total": 1},
  "traceId": "trace_..."
}
```

#### GET /api/v1/patients/{patientId}/alerts

获取患者告警历史。

**参数：**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| start | string(ISO8601) | 否 | 24h 前 | 起始时间 |
| end | string(ISO8601) | 否 | 当前 | 结束时间 |
| severity | string | 否 | 全部 | 过滤：WARNING / CRITICAL / EMERGENCY |
| limit | int | 否 | 100 | 返回条数上限 |

**响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "patientId": "P0001",
    "alerts": [
      {
        "alertId": "alert_...",
        "timestamp": "2026-07-21T09:00:00.000Z",
        "type": "TREND_ABNORMAL",
        "severity": "WARNING",
        "description": "HR 在 5 分钟内上升 18%",
        "mewsScore": 3
      }
    ]
  },
  "pagination": {"page": 1, "pageSize": 100, "total": 1},
  "traceId": "trace_..."
}
```

#### GET /api/v1/patients/{patientId}/snapshot

获取患者当前最新快照。

**响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "patientId": "P0001",
    "timestamp": "2026-07-21T10:00:00.000Z",
    "vitals": {
      "heartRate": 72,
      "sysBP": 120,
      "spo2": 98
    },
    "mewsScore": 1,
    "riskLevel": "STABLE",
    "activeDevices": ["monitor_001"],
    "devicesOnline": true
  },
  "traceId": "trace_..."
}
```

#### GET /api/v1/wards/{wardId}/overview

获取病区所有患者概览（为可视化层热力图提供数据）。

**响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "wardId": "ICU-EAST",
    "timestamp": "2026-07-21T10:00:00.000Z",
    "summary": {
      "totalPatients": 5,
      "stable": 3,
      "warning": 1,
      "critical": 1,
      "emergency": 0
    },
    "patients": [
      {
        "patientId": "P0001",
        "bedId": "ICU-101",
        "riskLevel": "STABLE",
        "mewsScore": 1,
        "lastUpdate": "2026-07-21T10:00:00.000Z",
        "vitalsSummary": {"heartRate": 72, "spo2": 98}
      },
      {
        "patientId": "P0002",
        "bedId": "ICU-102",
        "riskLevel": "CRITICAL",
        "mewsScore": 7,
        "lastUpdate": "2026-07-21T09:59:58.000Z",
        "vitalsSummary": {"heartRate": 130, "spo2": 88}
      }
    ]
  },
  "traceId": "trace_..."
}
```

### 4.3 错误响应格式

```json
{
  "code": 400,
  "message": "Bad Request",
  "error": {
    "detail": "Invalid patientId format: must be 'P' followed by 4 digits"
  },
  "traceId": "trace_..."
}
```

| HTTP 状态码 | 含义 |
|-------------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 429 | 请求频率超限 |
| 500 | 服务器内部错误 |

---

## 五、数据格式详解

### 5.1 时间格式

所有时间字段统一使用 **ISO 8601 UTC**：

```
标准格式: 2026-07-21T10:00:00.000Z
毫秒精度: 3 位小数
时区: 统一 UTC (Z 后缀)
```

### 5.2 traceId 格式

```
格式: "trace_" + UUID hex 24 位
示例: "trace_a1b2c3d4e5f6a7b8c9d0e1f"

用途: 全链路追踪，从 L2 Enricher 生成，贯穿 L3 → L4
L4 处理时: 请在您的日志和分析结果中延续 traceId
```

### 5.3 枚举值汇总

```yaml
RiskLevel:
  - STABLE
  - WARNING
  - CRITICAL
  - EMERGENCY

TrendDirection:
  - stable
  - rising
  - falling
  - rapid_change

AlertType:
  - MEWS_THRESHOLD
  - TREND_ABNORMAL
  - BASELINE_DEVIATION
  - SIGNAL_LOSS
  - CROSS_PARAMETER

AlertSeverity:
  - WARNING
  - CRITICAL
  - EMERGENCY

DataQuality:
  - good
  - fair
  - poor
```

---

## 六、集成指南

### 6.1 第一步：确认 Kafka 可达

```bash
# 确认 Kafka 连接
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ai.diagnostic.input \
  --group llm-agent \
  --from-beginning \
  --max-messages 5
```

### 6.2 第二步：消费诊断数据

```python
# Python 示例 — L4 团队的 Kafka 消费者
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "ai.diagnostic.input",
    bootstrap_servers="localhost:9092",
    group_id="llm-agent",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

for msg in consumer:
    diagnostic = msg.value
    patient_id = diagnostic["patientId"]
    risk_level = diagnostic["mews"]["riskLevel"]
    vitals = diagnostic["vitals"]

    print(f"[{patient_id}] Risk: {risk_level}, HR: {vitals['heartRate']['value']}")

    # 你的 LLM 诊断逻辑在这里...
    # 请延续 traceId: diagnostic["traceId"]
```

### 6.3 第三步：处理告警事件（可选）

```python
alert_consumer = KafkaConsumer(
    "ai.alerts",
    bootstrap_servers="localhost:9092",
    group_id="llm-agent-alerts",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

for msg in alert_consumer:
    alert = msg.value
    if alert["severity"] in ("CRITICAL", "EMERGENCY"):
        # 触发紧急诊断流程
        trigger_urgent_analysis(alert)
```

### 6.4 第四步：查询历史数据（可选）

```python
import httpx

# 查询患者过去 1 小时的心率和血氧数据
response = httpx.get(
    "http://localhost:8000/api/v1/patients/P0001/vitals",
    params={
        "start": "2026-07-21T09:00:00.000Z",
        "end": "2026-07-21T10:00:00.000Z",
        "parameters": "heartRate,spo2",
    },
)
data = response.json()
```

---

## 七、FAQ

### Q: L4 是否需要处理 Kafka 消息的顺序？

A: 不需要。`ai.diagnostic.input` 每个消息是**自包含的**（包含了当前时间窗口的全部信息），消息之间不依赖顺序。但如果需要一致性，建议按 `patientId` 分区消费以保证同一患者的消息有序到达。

### Q: 消息频率是多少？

A: 取决于设备数据频率和 Flink 窗口配置。默认：每 2 秒一个窗口，每患者每窗口输出一条消息。4 个设备同时发送时，每个患者平均每秒 1-2 条 `DiagnosticInput`。

### Q: L4 消费跟不上怎么办？

A: 
- Kafka 的持久化机制保证了消息不丢失
- 建议 L4 设置合适的 `max.poll.records`
- 如果压力过大，L4 可以只订阅 `ai.alerts`（数据量小得多），按需通过 REST API 查询详情

### Q: 如何确认数据链路完整性？

A: 使用 `traceId` 追踪：
```
L2 生成 traceId → 输出日志
L3 透传 traceId → 输出日志  
L4 延续 traceId → 您也可以在我们的日志中查到
```
遇到问题时提供 `traceId` 给我们排查。

### Q: REST API 的 QPS 限制是多少？

A: 开发环境不设限。生产环境建议控制在 100 QPS 以内，超过返回 429。

---

## 附录：快速 API 测试

```bash
# 测试 API 是否运行
curl http://localhost:8000/health

# 查询患者 P0001 最新快照
curl http://localhost:8000/api/v1/patients/P0001/snapshot

# 查询病区概览
curl http://localhost:8000/api/v1/wards/ICU-EAST/overview

# 查询历史 MEWS
curl "http://localhost:8000/api/v1/patients/P0001/mews?start=2026-07-21T00:00:00.000Z&limit=5"
```

> **版本：** v1.0  
> **最后更新：** 2026-07-21  
> **维护者：** L3 开发团队
