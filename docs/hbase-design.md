# HBase 数据存储设计

> 供 L4（LLM Agent）直接读取的数据层  
> 替代 InfluxDB 作为 AI 的历史数据源

---

## 设计原则

1. **AI 读取优先** — Row Key 设计让 AI 按患者+时间范围高效 Scan
2. **宽表设计** — 减少 AI 的 Join 操作，单行包含所有需要的信息
3. **冗余存储** — 同一份数据同时写 InfluxDB（API 查询）和 HBase（AI 读取）

---

## 表结构

### 1. 表 `vitals` — 体征综合数据表（核心）

AI 读取患者历史体征、MEWS 评分、趋势分析的主表。

| 项目 | 说明 |
|------|------|
| **表名** | `vitals` |
| **Row Key** | `{patientId}_{reverseTimestamp}` |
| **列族** | `v` (vitals), `m` (mews), `d` (diagnostic) |
| **TTL** | 30 天 |

#### Row Key 设计

```
格式: {patientId}_{Long.MAX_VALUE - timestamp_ms}

示例:
  P0001_18446744073709551615  ← 最新数据
  P0001_18446744073709550615  ← 较早数据

优点:
  - 按 patientId 前缀 Scan 即可获取单个患者全部历史
  - 时间戳反转使 Scan 结果从新到旧排序
  - AI 取最新数据: scan 'vitals', {STARTROW=>'P0001_', LIMIT=>1}
  - AI 取时间范围: 计算两个 reverseTimestamp 做 range scan
```

#### 列族 `v` (vitals) — 体征值

| 列限定符 | 类型 | 说明 | 示例 |
|---------|------|------|------|
| `v:heartRate` | double | 心率 | 72 |
| `v:sysBP` | double | 收缩压 | 120 |
| `v:diaBP` | double | 舒张压 | 80 |
| `v:spo2` | double | 血氧饱和度 | 98 |
| `v:respiratoryRate` | double | 呼吸频率 | 16 |
| `v:temperature` | double | 体温 | 36.8 |
| `v:deviceIds` | string | 活跃设备列表 | "monitor_001,temp_001" |

#### 列族 `m` (mews) — MEWS 评分

| 列限定符 | 类型 | 说明 | 示例 |
|---------|------|------|------|
| `m:totalScore` | int | MEWS 总分 | 1 |
| `m:riskLevel` | string | 风险等级 | "STABLE" |
| `m:hrScore` | int | 心率评分 | 0 |
| `m:bpScore` | int | 血压评分 | 0 |
| `m:rrScore` | int | 呼吸评分 | 0 |
| `m:tempScore` | int | 体温评分 | 0 |

#### 列族 `d` (diagnostic) — 趋势与诊断

| 列限定符 | 类型 | 说明 | 示例 |
|---------|------|------|------|
| `d:hrTrend` | string | 心率趋势 | "stable" |
| `d:hrChangeRate` | double | 心率变化率(%) | 1.2 |
| `d:bpTrend` | string | 血压趋势 | "stable" |
| `d:spo2Trend` | string | 血氧趋势 | "falling" |
| `d:anomalies` | string | 异常列表(JSON) | `[{"param":"spo2","type":"rapid_change"}]` |
| `d:timestamp` | string | 数据时间(ISO8601) | "2026-07-21T10:00:00.000Z" |
| `d:traceId` | string | 链路追踪ID | "trace_xxx" |
| `d:dataQuality` | string | 信号质量 | "good" |

### 2. 表 `alerts` — 告警事件表

| 项目 | 说明 |
|------|------|
| **表名** | `alerts` |
| **Row Key** | `{patientId}_{reverseTimestamp}` |
| **列族** | `a` |
| **TTL** | 90 天 |

#### 列族 `a` (alert)

| 列限定符 | 类型 | 说明 | 示例 |
|---------|------|------|------|
| `a:type` | string | 告警类型 | "MEWS_THRESHOLD" |
| `a:severity` | string | 严重程度 | "CRITICAL" |
| `a:mewsScore` | int | 触发时的 MEWS | 7 |
| `a:description` | string | 告警描述 | "HR 降至 42/min..." |
| `a:triggerParam` | string | 触发参数 | "heartRate" |
| `a:triggerValue` | double | 触发值 | 42 |
| `a:suggestedAction` | string | 建议操作 | "立即检查" |
| `a:timestamp` | string | 告警时间 | "2026-07-21T10:00:00.000Z" |
| `a:traceId` | string | 链路追踪ID | "trace_xxx" |

---

## AI 读取示例

### Python 客户端读取（给 L4 团队的参考代码）

```python
import happybase
import json
from datetime import datetime

# 连接 HBase Thrift（Docker 内）
connection = happybase.Connection('localhost', 9090)
connection.open()

# ── 1. 获取患者 P0001 最新状态 ──
table = connection.table('vitals')
rows = table.scan(
    row_prefix=b'P0001_',
    limit=1,
    columns=['v', 'm']
)
for key, data in rows:
    print(f"MEWS={data[b'm:totalScore']}, Risk={data[b'm:riskLevel']}")
    print(f"HR={data[b'v:heartRate']}, BP={data[b'v:sysBP']}/{data[b'v:diaBP']}")

# ── 2. 获取患者 P0001 过去 1 小时所有体征 ──
# 计算 reverseTimestamp 范围
now_ms = int(datetime.utcnow().timestamp() * 1000)
one_hour_ago_ms = now_ms - 3600000
max_long = 2**63 - 1
start_key = f"P0001_{max_long - now_ms:020d}"
end_key = f"P0001_{max_long - one_hour_ago_ms:020d}"

rows = table.scan(
    start_row=start_key.encode(),
    end_row=end_key.encode(),
    columns=['v']
)
for key, data in rows:
    print(f"HR={data[b'v:heartRate']}, SpO2={data[b'v:spo2']}")

# ── 3. 获取告警历史 ──
alerts_table = connection.table('alerts')
rows = alerts_table.scan(row_prefix=b'P0001_', limit=10)
for key, data in rows:
    print(f"告警: {data[b'a:severity']} - {data[b'a:description']}")

connection.close()
```

---

## 与现有数据流的关系

```
设备模拟器 → 采集层 → Kafka → Flink ──→ InfluxDB ←── API 查询（给可视化用）
                                    └──→ HBase    ←── AI 读取（给 LLM 用）
```

Flink 同时写入两个存储：
- **InfluxDB**: 供 L5 可视化层实时查询（短期、高性能）
- **HBase**: 供 L4 LLM Agent 大数据量读取（历史趋势、批量分析）
