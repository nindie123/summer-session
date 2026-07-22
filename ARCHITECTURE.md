# 医院实时生命体征监护系统 — 系统架构设计

> 版本：v1.1  
> 设计范围：L1 (设备模拟器) → L2 (数据采集层) → L3 (实时计算层)  
> 接口输出：L4 (LLM Agent) 消费接口契约  
> 当前状态：L3 实时计算由 bridge.py (Python) 替代 Flink 运行中，Flink 代码已写但待重建 jar  

---

## 当前架构实况（v1.1 实际部署）

```
                           ┌→ InfluxDB ─→ API(:8000) ─→ 仪表盘(/test)
                           │
设备模拟器 ──TCP:9001──→ 采集层 ──Kafka──→ bridge.py ──┼→ HBase (字符串可读格式)
   8患者 · 动态数据                      MEWS计算       │
   8种临床场景                           写入三路      └→ Kafka: ai.diagnostic.input ──→ L4 AI
```

### 与原始架构的偏差

| 项目 | 原始设计 | 当前实现 |
|------|---------|---------|
| **实时计算** | Flink (Java) | bridge.py (Python) |
| **HBase 写入** | Flink HBaseSink | bridge.py happybase |
| **告警产出** | Flink AlertExtractor | bridge.py → Kafka |
| **数据趋势** | Flink TrendAnalyzer | bridge.py 内联计算 |

### 偏差原因

1. **Docker 网络不稳定**（2026-07 国内镜像源大面积故障），Flink jar 无法重建
2. **Session Window 不触发**（原始 Flink 代码用了 session window，gap=2s 被持续数据重置）
3. **bridge.py 更快迭代**（50 行 Python vs 编译-部署-Flink 作业流程）

> 后续 Docker Hub 网络恢复后，可重建 Flink jar 切换回 Flink 运行，bridge.py 作为备用。
> Flink 代码在 `flink_computation/` 目录下完整保留。

---

## 目录

1. [架构总览](#一架构总览)
2. [Layer 1：C++ 设备模拟器](#二layer-1-c-设备模拟器)
3. [Layer 2：Python 数据采集层](#三layer-2-python-数据采集层)
4. [Layer 3：Flink 实时计算层](#四layer-3-flink-实时计算层)
5. [跨层数据流（端到端）](#五跨层数据流端到端)
6. [Layer 4 接口契约](#六layer-4-接口契约)
7. [部署架构](#七部署架构)
8. [关键设计决策记录](#八关键设计决策记录)

---

## 一、架构总览

### 1.1 总体分层

```
┌─────────────────────────────────────────────────────────────────────┐
│                    L5: 可视化交互层 (非本项目职责)                      │
│            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│            │ 护士站大屏    │  │ 医生工作站   │  │ 移动端 APP   │    │
│            └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│                   │                  │                  │           │
├───────────────────┼──────────────────┼──────────────────┼───────────┤
│                   │     REST API    │                  │           │
│  ┌────────────────▼──────────────────▼──────────────────▼────────┐ │
│  │              L4: LLM Agent 智能诊断层 (非本项目职责)            │ │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │ │
│  │   │ RAG 知识库   │  │ 临床指南库   │  │ 综合诊断推理     │  │ │
│  │   └──────────────┘  └──────────────┘  └──────────────────┘  │ │
│  │                     ▲                                        │ │
│  │          Kafka 消费 │  + REST 查询                            │ │
│  └─────────────────────┼────────────────────────────────────────┘ │
├────────────────────────┼──────────────────────────────────────────┤
│  ┌─────────────────────▼────────────────────────────────────────┐ │
│  │          L3: Flink 实时计算层 ★ (本项目职责)                   │ │
│  │                                                                 │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │ │
│  │  │设备融合   │→ │MEWS 评分 │→ │趋势分析  │→ │风险分级      │ │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────┬───────┘ │ │
│  │                                                    │         │ │
│  │  ┌─────────────────────────────────────────────────▼───────┐ │ │
│  │  │  Sink 输出层                                          │ │ │
│  │  │  ┌──────────────────┐  ┌──────────────┐  ┌──────────┐ │ │ │
│  │  │  │ Kafka: 诊断输入  │  │ Kafka: 告警  │  │InfluxDB  │ │ │ │
│  │  │  └──────────────────┘  └──────────────┘  └──────────┘ │ │ │
│  │  └─────────────────────────────────────────────────────────┘ │ │
│  │              ▲           ▲                                    │ │
│  │       消费 Kafka     REST 查询                                │ │
│  └──────────────────────┼─────────────────────────────────────────┘ │
│                         │                                          │
├─────────────────────────┼──────────────────────────────────────────┤
│  ┌──────────────────────▼────────────────────────────────────────┐ │
│  │          L2: Python 数据采集层 ★ (本项目职责)                   │ │
│  │                                                                 │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐ │ │
│  │  │TcpServer │→ │管道Pipeline│→ │Kafka     │→ │standardized.  │ │ │
│  │  │(asyncio) │  │(解析/验证)│  │Producer  │  │vitals Topic   │ │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                         ▲                                            │
├─────────────────────────┼────────────────────────────────────────────┤
│  ┌──────────────────────▼──────────────────────────────────────────┐ │
│  │          L1: C++ 设备模拟器 ★ (本项目职责)                        │ │
│  │                                                                   │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐   │ │
│  │  │监护仪    │  │呼吸机    │  │输液泵    │  │体温探头        │   │ │
│  │  │(1s/次)   │  │(2s/次)   │  │(5s/次)   │  │(30s/次)        │   │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └────────────────┘   │ │
│  │                                                                   │ │
│  │  协议: TCP + Length(4B Big-Endian) + JSON(UTF-8)                 │ │
│  │  认证: 连接后发送 auth_message 绑定 patientId + deviceId         │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────┘
```

### 1.2 项目范围

| 层 | 职责 | 技术栈 | 本项目 |
|----|------|--------|--------|
| L1 | 设备模拟器 | C++20 + ASIO | ✅ 负责 |
| L2 | 数据采集与标准化 | Python 3.12+ asyncio | ✅ 负责 |
| L3 | 实时计算与异常检测 | Java 17 + Flink 1.19 | ✅ 负责 |
| L4 | LLM Agent 智能诊断 | (留给他人) | ❌ 仅定义接口 |
| L5 | 可视化交互 | (留给他人) | ❌ 仅提供数据出口 |

### 1.3 核心数据流（一句话）

> **设备(L1) → TCP → 采集(L2) → Kafka → Flink(L3) → Kafka/InfluxDB → LLM Agent(L4)**

---

## 二、Layer 1：C++ 设备模拟器

### 2.1 设计目标

- 模拟 4 种临床设备，每种按固定频率推送体征数据
- 支持多设备并发连接（每个设备独立 TCP 连接）
- 连接时通过 `auth` 消息绑定 `patientId`（患者-设备关联）
- 支持异常数据注入（用于测试下游告警逻辑）
- 配置驱动（JSON 配置文件控制设备数量、参数范围、异常场景）

### 2.2 设备类型与参数

| 设备类型 | 标识 | 推送频率 | 产生的体征 |
|---------|------|---------|-----------|
| 监护仪 Monitor | `deviceType: "Monitor"` | 1s | HR, SBP, DBP, SpO2, RR |
| 呼吸机 Ventilator | `deviceType: "Ventilator"` | 2s | RR, TV (潮气量), PEEP, FiO2 |
| 输液泵 InfusionPump | `deviceType: "InfusionPump"` | 5s | 流速, 累计量, 药物名称 |
| 体温探头 TempSensor | `deviceType: "TempSensor"` | 30s | 体温 (T1, T2) |

### 2.3 通信协议

**传输层：** TCP 长连接  
**帧格式：** Length(4B Big-Endian) + JSON(UTF-8)

```
字节 0-3:    Payload Length (32-bit Big-Endian)
字节 4-N:    UTF-8 JSON Payload

示例:
  00 00 00 B4
  {"type":"vitals","deviceId":"monitor_001",...}
```

**消息类型：**

#### ① Auth（连接认证）

设备连接后立即发送，建立患者-设备绑定：

```json
{
  "type": "auth",
  "deviceId": "monitor_001",
  "deviceType": "Monitor",
  "patientId": "P0001",
  "secret": "sim_secret_2024"
}
```

采集层回复：
```json
{"type": "auth_ack", "status": "ok", "traceId": "trace_xxx"}
```

#### ② Vitals（体征数据）

```json
{
  "type": "vitals",
  "deviceId": "monitor_001",
  "deviceType": "Monitor",
  "patientId": "P0001",
  "timestamp": "2026-07-21T10:00:00.000Z",
  "sequence": 12345,
  "observations": [
    {"code": "8867-4", "name": "Heart Rate", "value": 72, "unit": "/min"},
    {"code": "8480-6", "name": "Systolic BP", "value": 120, "unit": "mmHg"},
    {"code": "8462-4", "name": "Diastolic BP", "value": 80, "unit": "mmHg"},
    {"code": "2708-6", "name": "Oxygen Saturation", "value": 98, "unit": "%"}
  ]
}
```

#### ③ Disconnect（断开通知）

```json
{"type": "disconnect", "deviceId": "monitor_001", "reason": "normal"}
```

### 2.4 患者绑定机制

```
绑定方式：每个设备连接时，在 auth 消息中携带 patientId
         一个 patientId 可对应多个设备
         一个设备只属于一个 patientId

生命周期：
  连接建立 → auth 消息 → PatientBinder 记录 deviceId ↔ patientId
  数据推送 → 每条消息携带 patientId（冗余，便于校验）
  断开连接 → PatientBinder 解除绑定（标记离线）

场景支持：
  转床：断开旧设备 → 新设备以新 patientId 连接
  加设备：已有 patientId 的新设备连接 → 自动加入
```

### 2.5 异常注入（测试能力）

模拟器支持配置触发特定异常场景：

| 场景 | 触发方式 | 效果 |
|------|---------|------|
| 心率异常 | 配置 `hr_anomaly` | HR 突降至 40 或升至 160 |
| 血氧骤降 | 配置 `spo2_drop` | SpO2 从 98% 逐渐降至 85% |
| 信号丢失 | 配置 `signal_loss` | 全参数输出 null |
| 设备离线 | 配置 `device_offline` | TCP 连接断开 |
| 数据延迟 | 配置 `data_lag` | timestamp 显著落后于当前时间 |

### 2.6 模块设计

```
device_simulator/
├── src/
│   ├── main.cpp                  # 入口：读取配置 → 启动设备线程池
│   ├── Simulator.h/cpp           # 模拟器主控：管理所有设备线程
│   ├── DeviceConnection.h/cpp    # 单设备连接：TCP 连接 + 帧编码 + 发送
│   ├── DeviceModel.h/cpp         # 设备模型基类：数据生成接口
│   ├── devices/
│   │   ├── MonitorModel.h/cpp    # 监护仪数据生成器
│   │   ├── VentilatorModel.h/cpp # 呼吸机数据生成器
│   │   ├── InfusionPumpModel.h/cpp
│   │   └── TempSensorModel.h/cpp
│   ├── FrameProtocol.h/cpp       # Length + JSON 帧编码
│   ├── AnomalyInjector.h/cpp     # 异常注入引擎
│   └── Config.h/cpp              # 配置加载
└── config/
    └── devices.json              # 设备配置（设备数量、参数范围、连接目标）
```

### 2.7 依赖

- C++20 编译器 (GCC 12+ / MSVC 2022+)
- ASIO（独立版，非 Boost）
- nlohmann/json（JSON 解析）
- CMake 3.20+

---

## 三、Layer 2：Python 数据采集层

### 3.1 设计原则

> **"只管搬运，不管加工"** — 采集层是无状态管道，负责连接、解析、验证、标准化、投递，所有跨设备、跨时间的计算交给 Flink。

### 3.2 与本项目协议的适配说明

由于 L1 是我们的 C++ 模拟器（非真实医疗设备），L2 的设计与完整版架构文档相比做以下简化：

| 完整版功能 | 本项目处理 | 理由 |
|-----------|-----------|------|
| HL7/DICOM 解析 | ❌ 不需要 | 模拟器只有 JSON 数据 |
| MLLP 帧解码器 | ❌ 不需要 | 只有 Length+JSON 一种帧格式 |
| LOINC 编码映射 | ⚡ 简化 | 模拟器已携带 code 字段，直接透传 |
| 多协议适配器 | ⚡ 简化 | 只有 JSON Parser |
| 设备管理 CRUD | ❌ 不实现 | 用配置文件替代 |
| 连接白名单 | ⚡ 简化为可配置 IP 段 | 开发环境 |

保留完整设计中的核心抽象（管道模式、注册表、验证链、死信队列），去掉 HL7/DICOM 等实际项目不需要的复杂度。

### 3.3 核心处理流程

```
TCP 连接建立
    │
    ▼
┌──────────────┐     接收字节流
│ TcpServer    │─────────────────────────┐
│ (asyncio)    │                          │
│ 每个连接一个 │                          │
│ Coroutine    │                          │
└──────┬───────┘                          │
       │                                  │
       ▼                                  ▼
┌──────────────┐                 ┌──────────────────┐
│ ConnManager  │                 │ FramingDecoder   │
│ 连接注册/保活│                 │ Length + JSON    │
│ deviceId ↔   │                 │ 处理粘包/拆包    │
│ connection   │                 └────────┬─────────┘
└──────┬───────┘                          │
       │                                  ▼
       │                          ┌──────────────────┐
       │                          │   Auth Handler   │
       │                          │ type="auth" 验证  │
       │                          │ PatientBinder 注册│
       │                          │ 回复 auth_ack    │
       │                          └────────┬─────────┘
       │                                   │ (后续消息)
       │                                   ▼
       │                          ┌──────────────────┐
       │                          │  Pipeline 处理链   │
       │                          │                   │
       │                          │  ① Parser         │
       │                          │     JSON → Dict   │
       │                          │                   │
       │                          │  ② Validator      │
       │                          │     Schema + Range │
       │                          │     ↓ fail→DLQ    │
       │                          │                   │
       │                          │  ③ Normalizer     │
       │                          │     时间标准化     │
       │                          │     单位统一       │
       │                          │                   │
       │                          │  ④ Enricher       │
       │                          │     追加 traceId  │
       │                          │     serverTimestamp│
       │                          └────────┬─────────┘
       │                                   │
       │                                   ▼
       │                          ┌──────────────────┐
       │                          │   Router &        │
       │                          │   Kafka Producer  │
       │                          │   按类型分发      │
       │                          └────────┬─────────┘
       │                                   │
       │                                   ▼
       │                          ┌──────────────────┐
       │                          │ standardized.    │
       │                          │ vitals (Topic)   │
       │                          └──────────────────┘
```

### 3.4 Kafka Topic 设计

| Topic | 分区 | 保留 | 说明 |
|-------|------|------|------|
| `standardized.vitals` | 4 | 7天 | 标准化体征数据，供 Flink 消费 |
| `device.status` | 2 | 3天 | 设备连接状态变更 |
| `validation.alert` | 2 | 3天 | 验证层告警（格式错误、值域越界） |
| `dead.letter.queue` | 2 | 30天 | 无法处理的消息（人工回放） |

**分区策略：** `patientId.hashCode() % numPartitions` — 保证同一患者数据有序。

### 3.5 与 L4 的边界（采集层视角）

采集层不感知 L4 的存在。它只产出 `standardized.vitals` Topic，Flink 消费后做计算再产出供 L4 消费的数据。

### 3.6 模块设计

```
data_collector/
├── src/
│   ├── main.py                      # 入口：启动 TcpServer + Health API
│   ├── server/
│   │   ├── tcp_server.py            # TCP 接入器 (asyncio.start_server)
│   │   └── conn_manager.py          # 连接管理器：注册/保活/注销
│   ├── pipeline/
│   │   ├── frame_decoder.py         # Length+JSON 帧解码器
│   │   ├── parser.py                # JSON 解析器
│   │   ├── validator.py             # Schema + Range 验证器
│   │   ├── normalizer.py            # 时间/单位标准化
│   │   ├── enricher.py              # metadata 富化
│   │   └── pipeline.py              # Pipeline 编排
│   ├── kafka/
│   │   ├── producer.py              # Kafka 异步生产者
│   │   ├── router.py                # 消息路由
│   │   └── dlq.py                   # 死信队列
│   ├── state/
│   │   ├── patient_binder.py        # deviceId ↔ patientId 绑定
│   │   ├── device_tracker.py        # 设备在线跟踪
│   │   └── health_checker.py        # 连接健康检查
│   ├── models/
│   │   ├── patient_vital_record.py  # 统一数据模型
│   │   └── enums.py                 # 枚举定义
│   └── observability/
│       ├── metrics.py               # Prometheus 指标
│       └── logger.py                # 结构化日志
└── tests/
```

---

## 四、Layer 3：Flink 实时计算层

### 4.1 设计目标

- 消费 `standardized.vitals` Topic，按 `patientId` 对齐多设备数据
- 时间窗口内融合多来源体征 → 计算 MEWS 评分
- 趋势分析 + 异常检测 → 风险分级
- 输出到 Kafka（L4 消费）+ InfluxDB（历史查询）

### 4.2 Job 拓扑

```
Kafka Source: standardized.vitals
  │  (Avro 反序列化为 PatientVitalRecord POJO)
  │
  ▼
KeyBy: patientId
  │
  ▼
Session Window (gap = 2s)
  │  将同一患者 2s 内的多设备消息融合
  │
  ▼
DeviceFusionFunction
  │  合并同一时间窗口内多个设备的 observations
  │  输出 PatientSnapshot（患者当前多参数快照）
  │
  ├──────────────────────────────────────┐
  ▼                                      │
MewsCalculator                           │
  │  HR + SBP + RR + Temp → MEWS 评分   │
  ▼                                      │
TrendAnalyzer                            │
  │  与历史状态对比 → 变化率 + 趋势方向  │
  ▼                                      │
RiskClassifier                           │
  │  MEWS + Trend → 风险分级             │
  │                                      │
  ▼                                      ▼
┌──────────────────┐  ┌──────────────────┐
│ Main Output      │  │ Side Output      │
│ Kafka Sink       │  │ Kafka Sink       │
│ ai.diagnostic.   │  │ ai.alerts        │
│ input            │  │ (severity>阈值时) │
└──────────────────┘  └──────────────────┘
       │
       ▼
┌──────────────────┐
│ InfluxDB Sink    │
│ (所有时间点)     │
└──────────────────┘
```

### 4.3 核心数据模型（Flink 侧）

#### PatientSnapshot（患者快照）

```json
{
  "patientId": "P0001",
  "timestamp": "2026-07-21T10:00:00.000Z",
  "vitals": {
    "heartRate": {"value": 72, "unit": "/min", "deviceId": "monitor_001", "timestamp": "..."},
    "sysBP": {"value": 120, "unit": "mmHg", "deviceId": "monitor_001", "timestamp": "..."},
    "spo2": {"value": 98, "unit": "%", "deviceId": "monitor_001", "timestamp": "..."},
    "respiratoryRate": {"value": 16, "unit": "/min", "deviceId": "ventilator_001", "timestamp": "..."},
    "temperature": {"value": 36.8, "unit": "°C", "deviceId": "temp_001", "timestamp": "..."}
  },
  "activeDevices": ["monitor_001", "ventilator_001", "temp_001"]
}
```

#### MewsScore

```json
{
  "patientId": "P0001",
  "timestamp": "2026-07-21T10:00:00.000Z",
  "totalScore": 1,
  "components": {
    "heartRate": 0,
    "sysBP": 0,
    "respiratoryRate": 0,
    "temperature": 0,
    "avpu": 0
  },
  "riskLevel": "STABLE"
}
```

#### AlertEvent

```json
{
  "alertId": "alert_00112233-4455",
  "patientId": "P0001",
  "timestamp": "2026-07-21T10:00:00.000Z",
  "type": "MEWS_THRESHOLD",
  "severity": "CRITICAL",
  "mewsScore": 8,
  "trigger": {
    "parameter": "heartRate",
    "value": 42,
    "threshold": 45,
    "trend": "falling"
  },
  "description": "HR 降至 42/min，低于危急阈值，MEWS=8",
  "vitalSnapshot": {
    "heartRate": 42,
    "sysBP": 85,
    "spo2": 94
  },
  "traceId": "trace_...",
  "windowStart": "2026-07-21T09:59:50.000Z",
  "windowEnd": "2026-07-21T10:00:00.000Z"
}
```

### 4.4 计算逻辑详述

#### ① 多设备融合策略

```
输入：同一患者 2s 会话窗口内的所有消息
      {monitor: [HR, BP, SpO2], ventilator: [RR, TV], temp: [T1]}

融合规则：
  - 最近值优先：同一参数出现多次，取时间戳最新的
  - 缺值容忍：某些参数缺失不影响评分（如体温 30s 一次，非每秒都有）
  - 冲突处理：同一参数同一设备出现不同值 → 取有效标记的时间最新值

输出：一个 PatientSnapshot，包含该患者所有已知参数的最新值
```

#### ② MEWS 评分计算

```
MEWS (Modified Early Warning Score)

HR:  ≤40→3, 41-50→2, 51-100→0, 101-110→1, 111-129→2, ≥130→3
SBP: ≤70→3, 71-80→2, 81-100→1, 101-199→0, ≥200→2
RR:  ≤8→3, 9-11→1, 12-20→0, 21-25→1, 26-35→2, ≥36→3
Temp: ≤35.0→2, 35.1-36.0→1, 36.1-38.0→0, 38.1-38.5→1, ≥38.6→2
AVPU: Alert→0, Voice→1, Pain→2, Unresponsive→3 (模拟器暂不实现)

总分分级:
  0-4  → STABLE（稳定）
  5-6  → WARNING（预警）
  7-8  → CRITICAL（危急）
  ≥9   → EMERGENCY（濒死）

计算时机：每次 PatientSnapshot 更新时触发
```

#### ③ 趋势分析

```
短期趋势（5min 滑动窗口）：
  - 计算均值与当前值的偏差百分比
  - |偏差|>15% → 标记为 "rapid_change"

中期趋势（30min 滑动窗口）：
  - 线性回归计算斜率
  - 持续上升/下降 → 标记趋势方向

长期基线（患者自身基线，累积窗口）：
  - 存储患者各参数的正常范围（移动平均 ± 2σ）
  - 当前值偏离基线 > 3σ → 标记为 "baseline_deviation"
```

#### ④ 风险分级融合

```
最终 RiskLevel 由 MEWS + 趋势联合决定：

  MEWS 0-4 + 趋势稳定 → STABLE
  MEWS 0-4 + 快速变化 → WARNING（趋势作为提前预警）
  MEWS 5-6 + 任意     → WARNING
  MEWS 7-8 + 任意     → CRITICAL
  MEWS ≥9             → EMERGENCY
  基线偏离 + MEWS 3+  → 提升一级
```

### 4.5 状态管理

| 状态 | 类型 | 说明 |
|------|------|------|
| PatientSnapshot | ValueState<PatientSnapshot> | 患者当前综合状态 |
| MEWS Score | ValueState<MewsScore> | 最新 MEWS 评分 |
| TrendHistory | MapState<param, List<value>> | 用于趋势计算的最近 N 个值 |
| BaselineState | ValueState<Baseline> | 患者参数基线（均值 + 标准差） |

Flink 的 Checkpoint 机制保证状态容错。

### 4.6 输出数据流

| 输出 | 目标 | 格式 | 触发条件 |
|------|------|------|---------|
| 诊断输入 | Kafka: `ai.diagnostic.input` | DiagnosticInput JSON | 每次 PatientSnapshot 更新 |
| 告警事件 | Kafka: `ai.alerts` | AlertEvent JSON | 风险 ≥ WARNING |
| 时序记录 | InfluxDB: `vitals` measurement | 时序点 | 每次 PatientSnapshot 更新 |
| 评分记录 | InfluxDB: `mews` measurement | 时序点 | 每次 MEWS 计算 |

### 4.7 REST API 查询服务

Layer 3 额外提供一个 **FastAPI 查询服务**（Python），供 L4/L5 查询历史数据：

```
GET /api/v1/patients/{patientId}/vitals?start=&end=
  → 指定患者时间范围内的体征时序数据（查询 InfluxDB）

GET /api/v1/patients/{patientId}/mews?start=&end=
  → MEWS 评分历史

GET /api/v1/patients/{patientId}/alerts?start=&end=
  → 告警历史

GET /api/v1/patients/{patientId}/snapshot
  → 患者当前最新快照

GET /api/v1/wards/{wardId}/overview
  → 病区所有患者概览（风险热力图数据源）
```

---

## 五、跨层数据流（端到端）

### 5.1 一条数据从设备到输出的完整旅程

```
时间 设备(L1)             采集层(L2)                   Flink(L3)                  L4消费方
─── ─────────           ─────────────               ────────────               ─────────
T+0  Monitor 发送
     HR=72, BP=120/80
     → TCP Write

T+1                   FramingDecoder 收到 150B
                      提取完整消息帧

T+2                   Parser: JSON 解析
                      Validator: HR=72 in [0,300] ✅
                      Normalizer: 时间格式统一
                      Enricher: 追加 traceId
                      KafkaProducer: 发送到
                      standardized.vitals

T+3                                                       Flink 消费 standardized.vitals
                                                         KeyBy patientId=P0001

T+4                                                       Session Window 等待 2s
                                                         等待同患者其他设备数据

T+5                                                       Ventilator 数据到达 (RR=16)
                                                         → 触发窗口计算

T+6                                                       DeviceFusionFunction:
                                                         HR=72, BP=120/80, RR=16
                                                         → PatientSnapshot

T+7                                                       MewsCalculator:
                                                         HR=72→0分, SBP=120→0分
                                                         RR=16→0分, Temp=36.8→0分
                                                         → MEWS=1 (STABLE)

T+8                                                       TrendAnalyzer:
                                                         与历史对比 → 趋势稳定

T+9                                                       RiskClassifier:
                                                         MEWS=1 + 稳定 → STABLE
                                                         不触发告警

T+10                                                      Kafka Sink: DiagnosticInput
                                                         InfluxDB Sink: 时序点写入

T+11                                                                             LLM Agent 消费
                                                                                ai.diagnostic.input
                                                                                → 综合分析
```

### 5.2 延迟预算

| 阶段 | 预算 | 测量方式 |
|------|------|---------|
| L1 → L2 (网络) | < 10ms | serverTimestamp - deviceTimestamp |
| L2 处理 | < 5ms | pipeline 内计时 |
| L2 → Kafka | < 10ms | Kafka 生产者回调 |
| Kafka → Flink | < 50ms | Flink 内 Source 延迟指标 |
| Flink 计算 | < 100ms | Flink 内算子计时 |
| Flink → Kafka (输出) | < 10ms | 生产者回调 |
| **端到端** | **< 200ms** | traceId 贯穿全链路 |

---

## 六、Layer 4 接口契约

### 6.1 概览

Layer 3 向 Layer 4（LLM Agent 层）提供以下数据接口：

| 接口类型 | 方式 | 内容 | 实时性 |
|---------|------|------|--------|
| 诊断输入流 | Kafka Topic `ai.diagnostic.input` | 患者综合状态 + MEWS + 趋势 | 实时（秒级） |
| 告警事件流 | Kafka Topic `ai.alerts` | 异常事件 + 风险分级 | 实时（秒级） |
| 历史数据查询 | REST API | 体征时序 / MEWS 历史 / 告警记录 | 近实时 |

### 6.2 DiagnosticInput Schema

Kafka Topic: `ai.diagnostic.input`  
Key: `patientId` (String)  
Value: JSON

```json
{
  "schemaVersion": "1.0",
  "messageId": "msg_...",
  "traceId": "trace_...",

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

  "anomalies": [
    {
      "parameter": "heartRate",
      "type": "rapid_change",
      "severity": "WARNING",
      "description": "HR 在 5 分钟内从 72 升至 110",
      "details": {"previousValue": 72, "currentValue": 110, "timeWindowMin": 5}
    }
  ],

  "activeDevices": ["monitor_001", "ventilator_001", "temp_001"],
  "dataQuality": {"overall": "good", "signalLost": false, "artifactsDetected": false}
}
```

**字段说明：**

| 字段 | 说明 |
|------|------|
| `vitals.*.trend` | `"stable" | "rising" | "falling" | "rapid_change"` — 趋势方向 |
| `vitals.*.changeRate` | 单位时间变化率（正=上升，负=下降） |
| `vitals.*.isAnomalous` | 是否偏离基线 > 3σ |
| `mews.riskLevel` | `"STABLE" | "WARNING" | "CRITICAL" | "EMERGENCY"` |
| `anomalies[]` | 当前窗口检测到的所有异常（可为空数组） |
| `dataQuality` | 信号质量标记，L4 可据此决定是否采纳数据 |

### 6.3 AlertEvent Schema

Kafka Topic: `ai.alerts`  
Key: `patientId` (String)  
Value: JSON

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

**告警类型枚举：**

| type | 含义 | 触发条件 |
|------|------|---------|
| `MEWS_THRESHOLD` | MEWS 评分超阈 | MEWS ≥ 5 触发 WARNING，≥ 7 触发 CRITICAL |
| `TREND_ABNORMAL` | 趋势异常 | 任一参数 5min 变化率 > 15% |
| `BASELINE_DEVIATION` | 基线偏离 | 参数偏离患者自身基线 > 3σ |
| `SIGNAL_LOSS` | 信号丢失 | 设备数据中断超过预期时间 |
| `CROSS_PARAMETER` | 跨参数异常 | 多参数联合异常模式（如 HR↓+BP↓） |

**告警去重：** Flink 侧对同一患者相同 type 的告警做 30s 静默去重，避免告警风暴。

### 6.4 REST API

**Base URL:** `http://<api-gateway>:8000/api/v1`

| 端点 | 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|------|
| `/patients/{pid}/vitals` | GET | start, end, limit | 时序数组 | 患者体征历史 |
| `/patients/{pid}/mews` | GET | start, end, limit | MEWS 时序 | MEWS 评分历史 |
| `/patients/{pid}/alerts` | GET | start, end, severity | 告警列表 | 告警事件历史 |
| `/patients/{pid}/snapshot` | GET | — | PatientSnapshot | 患者当前状态 |
| `/wards/{wardId}/overview` | GET | — | 患者风险列表 | 病区概览（热力图数据源） |

**统一响应格式：**

```json
{
  "code": 200,
  "message": "success",
  "data": { ... },
  "pagination": {"page": 1, "pageSize": 100, "total": 1234},
  "traceId": "trace_..."
}
```

### 6.5 L4 集成步骤（给对方的指南）

```
1. 确认 Kafka 连接信息（bootstrap_servers）
2. 订阅 Topic `ai.diagnostic.input`（group_id = "llm-agent"）
3. 订阅 Topic `ai.alerts`（如需要实时告警推送）
4. 按 DiagnosticInput Schema 解析消息
5. 可选：调用 REST API 查询历史数据辅助诊断
6. 注意：traceId 贯穿全链路，L4 处理时请延续使用
```

---

## 七、部署架构

### 7.1 开发环境（Docker Compose）

```
┌────────────────────────────────────────────────────────────┐
│                  开发笔记本 / 服务器                          │
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Simulator │  │ Collector│  │ Flink    │  │ API      │   │
│  │ (C++进程) │  │ (Python) │  │ (Job)    │  │ Gateway  │   │
│  └─────┬────┘  └─────┬────┘  └─────┬────┘  └─────┬────┘   │
│        │              │              │              │        │
│        └──────────────┼──────────────┼──────────────┘        │
│                       │              │                       │
│                 ┌─────▼──────┐ ┌─────▼──────┐                │
│                 │  Kafka     │ │  InfluxDB  │                │
│                 │  (Container)│ │  (Container)│               │
│                 └────────────┘ └────────────┘                │
│                                                            │
│  ┌──────────────┐  ┌──────────────┐                         │
│  │  Redis       │  │  MySQL       │                         │
│  │  (Container) │  │  (Container) │                         │
│  └──────────────┘  └──────────────┘                         │
└────────────────────────────────────────────────────────────┘
```

### 7.2 容器编排

| 服务 | 镜像 | 端口 | 依赖 |
|------|------|------|------|
| simulator | 原生进程（不容器化） | — | 直接连接 collector |
| collector | python:3.12-slim | 9001(TCP), 8080(健康检查) | Kafka, Redis |
| flink-jobmanager | flink:1.19-java17 | 8081(Web UI) | Kafka |
| flink-taskmanager | flink:1.19-java17 | — | Kafka |
| api-gateway | python:3.12-slim | 8000(REST) | InfluxDB |
| kafka | confluentinc/cp-kafka:7.6 | 9092 | — |
| influxdb | influxdb:2.7 | 8086 | — |
| redis | redis:7-alpine | 6379 | — |
| mysql | mysql:8.0 | 3306 | — |

### 7.3 最低硬件要求

- CPU: 4 核 (x86_64)
- 内存: 8 GB
- 磁盘: 50 GB
- 系统: Linux / Windows (WSL2)

---

## 八、关键设计决策记录

### ADR-1：为何 C++ 做模拟器而非 Python

| 选项 | 决策 |
|------|------|
| C++ | **选定** — 贴近真实医疗设备（多为 C/C++ 固件），展示语言能力，高精度定时 |
| Python | 开发快但定时精度受限，不符合"设备模拟"的定位 |
| Go | 可选但团队不熟悉 |

### ADR-2：为何简化采集层的协议适配

| 选项 | 决策 |
|------|------|
| **保留完整的多协议架构** | ❌ 过度设计，模拟器只有 JSON |
| **只实现 JSON Parser** | ✅ 接口保留（IParser），框架留好，实现只做一个 |

**结论：** 保留完整架构的接口抽象（IConnector, IFrameDecoder, IDataParser, IValidator），但只实现 JSON 版本。未来接入真实设备时加实现类即可。

### ADR-3：Flink 使用 Session Window 而非 Tumbling Window

| 窗口类型 | 理由 |
|---------|------|
| Tumbling Window | 固定间隔窗口，不同频率设备难以对齐 |
| **Session Window (gap=2s)** | **选定** — 天然适合不同频率设备的融合，设备发送间隔不固定时仍能正确分组 |
| Sliding Window | 计算量大，适合需求明确的情况 |

### ADR-4：为何用 InfluxDB 而非 HBase

| 选项 | 理由 |
|------|------|
| HBase | 运维复杂，小学期环境难以搭建 |
| **InfluxDB** | **选定** — Docker 一键部署，时序查询友好，HTTP API 方便集成 |

### ADR-5：患者绑定在 L2 而非 L1

| 位置 | 决策 |
|------|------|
| L1 硬编码 | ❌ 不符合"采集层管理绑定"的架构分层 |
| **L2 PatientBinder** | **选定** — 采集层通过 auth 消息维护 deviceId ↔ patientId 映射，Redis 缓存 |
| L3 Flink | ❌ 绑定属于元数据管理，不应在计算层解决 |

---

> **架构设计核心思想：**
>
> **L1 模拟真实设备，L2 管道搬运数据，L3 融合计算推理，L4 接口契约清晰。**
> **各层职责分明，通过 Kafka 解耦，独立开发独立部署。**
