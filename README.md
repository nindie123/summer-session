# summer-session — 医院实时生命体征监护系统

> 大三小学期项目 · 全容器化一键部署  
> 范围：L1 (设备模拟器) → L2 (数据采集) → L3 (Flink 实时计算) → L4 (LLM Agent 接口)

---

## 快速开始

### 前提

- Docker Desktop 已安装并运行

### 一键启动

```bash
cd docker
docker compose up -d
```

等待约 2 分钟让 HBase 就绪，然后：

```bash
# 创建 HBase 表（仅首次）
docker exec docker-hbase-1 bash -c '
  echo "create \"vitals\", {NAME=>\"v\"}, {NAME=>\"m\"}, {NAME=>\"d\"}" | /opt/apache/hbase-2.5.4/bin/hbase shell -n
  echo "create \"alerts\", {NAME=>\"a\"}" | /opt/apache/hbase-2.5.4/bin/hbase shell -n
'
```

之后所有服务（包括 Flink 作业）自动运行（`restart: unless-stopped`）。

### 停止

```bash
cd docker && docker compose down -v
```

---

## 访问入口

| 入口 | 地址 | 说明 |
|------|------|------|
| 大屏监控 | http://localhost:8000/bigscreen | WebSocket 实时推送，暗色主题大屏（推荐） |
| 趋势仪表盘 | http://localhost:8000/test | 8 患者实时体征卡片 |
| API 文档 | http://localhost:8000/docs | 交互式测试接口 |
| 病区概览 | http://localhost:8000/api/v1/wards/ICU-EAST/overview | 患者 MEWS 列表 |
| 大屏聚合 | http://localhost:8000/api/v1/bigscreen/overview | 全患者体征+MEWS 聚合数据 |
| Flink 监控 | http://localhost:8081 | Flink Web UI |
| Kafka 诊断数据 | ai.diagnostic.input | MEWS 评分结果（L4 消费） |

---

## 系统架构

```
                           ┌→ InfluxDB ─→ API(:8000) ─→ 大屏(/bigscreen)
                           │                              仪表盘(/test)
设备模拟器 ──TCP:9001──→ 采集层 ──Kafka──→ Flink ─────────┼→ HBase 2.5.4
   8患者 · 5设备/人          管道:解析/验证    MEWS计算     │   (字符串格式)
   restart: unless-stopped    标准化/投递    趋势/风险     └→ Kafka: ai.diagnostic.input
                                                告警        → ai.alerts
```

### 容器列表

| 容器 | 镜像 | 功能 | 自动重启 |
|------|------|------|---------|
| `device-simulator` | docker-base | 8 患者 × 5 设备动态数据模拟 | ✅ |
| `collector` | docker-collector | TCP :9001 接入，数据验证，Kafka 生产 | ✅ |
| `flink-job` | docker-flink-job:fixed | MEWS 计算 + 趋势分析 + 三路写入 | ❌ 一次性提交 |
| `jobmanager` | docker-flink-job | Flink 集群管理器 | ✅ |
| `taskmanager` | flink:1.19-java17 | Flink 任务执行器 | ✅ |
| `api-gateway` | docker-api-gateway | REST API + 大屏 + 仪表盘 | ✅ |
| `hbase` | hbase:2.5.4 | 时序数据存储（AI读取） | ✅ |
| `influxdb` | influxdb:2.7 | 时序数据存储（API查询） | ✅ |
| `kafka` | cp-kafka:7.5 | 消息队列 | ✅ |

---

## 8 患者场景

| 患者 | 场景 | 数据特征 |
|------|------|---------|
| P001 | 健康 | 温和正弦波动 |
| P002 | 高血压 | BP 150-200/90-120，大幅波浪起伏 |
| P003 | 心动过速 | HR 110-140，快速波动 |
| P004 | 心动过缓 | HR 40-55，低位慢波 |
| P005 | 低血氧 | SpO2 85-92%，周期性下降 |
| P006 | 发热 | TEMP 38.5-40°C，缓慢波动 |
| P007 | 恶化中 | HR↑ SpO2↓ BP↓ 长期漂移 |
| P008 | 重症 | 剧烈快速摆动，高危状态 |

### 5 种设备（每位患者各一）

| 设备 | 体征 | 频率 |
|------|------|------|
| HeartRate | HR | 1秒 |
| BloodPressure | SBP / DBP | 5秒 |
| SpO2Monitor | SpO2 | 2秒 |
| Respiratory | RR | 3秒 |
| Temperature | TEMP | 30秒 |

---

## 技术栈

| 组件 | 版本 | 运行方式 |
|------|------|---------|
| Docker | 29.6.1 | 全部容器化 |
| Kafka | 7.5.0 | Docker，4 partition |
| InfluxDB | 2.7 | Docker |
| HBase | **2.5.4** (阿里云镜像) | Docker，稳定版本 |
| Flink | **1.19.3** | **✅ 运行中** — MEWS 计算 + 数据写入 |
| Python | 3.12+ | 采集层 + API 网关 + 模拟器 |
| Java | 17 | Flink 实时计算作业 |

---

## 数据存储

| 存储 | 用途 | 数据格式 |
|------|------|---------|
| **InfluxDB** | API 查询 / 仪表盘 / 大屏 | Line Protocol（Flink 写入） |
| **HBase** | L4 LLM Agent 批量读取 | **字符串格式** `v:heartRate = "72.0"` |
| **Kafka** | 实时流 + 跨层通信 | JSON |

---

## HBase 读取示例（给 AI 团队）

```python
from happybase import Connection
conn = Connection('localhost', 9090)
table = conn.table('vitals')
for key, data in table.scan(row_prefix=b'P001_', limit=5):
    print(dict(data))
```

---

## 文件结构

```
summer_all1/
├── docker/docker-compose.yml         # 全部服务编排
├── docker/flink-submit.sh            # Flink 作业提交脚本
├── docker/flink-fix.Dockerfile       # Flink 修复构建文件
├── scripts/
│   ├── run_8_patients.py             # 8 患者动态数据模拟器
│   ├── bridge_simple.py              # (备用) Kafka→InfluxDB+HBase 桥接器
│   ├── bigscreen_demo.html           # 大屏可视化 Demo
│   ├── test_dashboard.html           # 实时体征仪表盘
│   └── read_hbase.py                 # HBase 数据可读查看
├── device_simulator/                 # L1: C++ 设备模拟器
├── data_collector/                   # L2: Python 数据采集层
├── flink_computation/                # L3: Java Flink 实时计算（已部署运行）
├── api_gateway/                      # REST API + 大屏 + 仪表盘
├── docs/
│   ├── hbase-design.md               # HBase 表结构
│   └── layer4-interface-contract.md  # L4 接口契约
└── ARCHITECTURE.md                   # 系统架构设计
```
