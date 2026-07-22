# summer-session — 医院实时生命体征监护系统

> 大三小学期项目 · 全容器化一键部署  
> 范围：L1 (设备模拟器) → L2 (数据采集) → L3 (实时计算) → L4 (LLM Agent 接口)

---

## 快速开始

### 一键启动

```bash
cd D:\little\summer_all1\docker
docker compose up -d
```

等待约 60 秒让 HBase 就绪，然后启动桥接器和模拟器：

```bash
# 桥接器（Kafka → InfluxDB + HBase）
docker exec docker-collector-1 pip install -q happybase
docker cp D:\little\summer_all1\scripts\bridge.py docker-collector-1:/app/bridge.py
docker exec docker-collector-1 sh -c "KAFKA_BOOTSTRAP_SERVERS=kafka:9092 INFLUXDB_URL=http://influxdb:8086 HBASE_THRIFT_HOST=hbase HBASE_THRIFT_PORT=9090 nohup python3 -u //app//bridge.py > //tmp//bridge.log 2>&1 &"

# 8 患者模拟器
PYTHONIOENCODING=utf-8 /c/Users/zhllj/anaconda3/python.exe D:/little/summer_all1/scripts/run_8_patients.py
```

### 停止

```bash
cd D:\little\summer_all1\docker
docker compose down -v
```

---

## 访问入口

| 入口 | 地址 | 说明 |
|------|------|------|
| 趋势仪表盘 | http://localhost:8000/test | 8 患者体征折线图（推荐） |
| API 文档 | http://localhost:8000/docs | 交互式测试接口 |
| 病区概览 | http://localhost:8000/api/v1/wards/ICU-EAST/overview | 患者 MEWS 列表 |
| Kafka 诊断数据 | ai.diagnostic.input | MEWS 评分结果（L4 消费） |
| Flink 监控 | http://localhost:8081 | （可选项） |

---

## 当前架构

```
                           ┌→ InfluxDB ─→ API(:8000) ─→ 仪表盘(/test)
                           │
设备模拟器 ──TCP:9001──→ 采集层 ──Kafka──→ bridge.py ──┼→ HBase (可读字符串格式)
   8患者 · 动态数据                      MEWS计算       │
   8种临床场景                           写入两路      └→ Kafka: ai.diagnostic.input ──→ L4 AI
```

### 各层职责

| 层 | 技术 | 功能 |
|----|------|------|
| **设备模拟器** | Python 脚本 (`run_8_patients.py`) | 8 个患者的实时体征模拟，正弦波动 + 临床趋势 |
| **采集层** | Python asyncio (`data_collector/`) | TCP :9001 接入，协议解析，数据验证，Kafka 生产 |
| **桥接器** | Python aiokafka (`bridge.py`) | 从 Kafka 消费，计算 MEWS 评分，写入 InfluxDB + HBase + Kafka 诊断 Topic |
| **Flink (备选)** | Java Flink 1.19 | 与 bridge.py 功能重叠，当前未启用（网络问题无法构建新镜像） |
| **API 网关** | Python FastAPI | REST 查询接口，Swagger 文档 |
| **仪表盘** | 纯 HTML/Chart.js | `/test` 页面，折线图展示 8 患者趋势，5 秒自动刷新 |

---

## 数据存储

| 存储 | 用途 | 数据格式 |
|------|------|---------|
| **InfluxDB** | API 查询 / 仪表盘数据源 | Line Protocol，label 格式 |
| **HBase** | L4 (LLM Agent) 批量读取 | **字符串格式可读**（v:heartRate = "75.8"） |
| **Kafka** | 实时流 + 跨层通信 | JSON |

---

## 8 患者场景

| 患者 | 场景 | 数据特征 |
|------|------|---------|
| P001 | 健康 | 温和正弦波动，偶尔小波动 |
| P002 | 高血压 | 血压 150-200/90-120，大幅波浪式起伏 |
| P003 | 心动过速 | 心率 110-140，快速波动 |
| P004 | 心动过缓 | 心率 40-55，低位慢波 |
| P005 | 低血氧 | SpO2 85-92%，周期性下降 |
| P006 | 发热 | 体温 38.5-40°C，缓慢波动 |
| P007 | 恶化中 | HR↑ SpO2↓ BP↓ 长期漂移恶化 |
| P008 | 重症 | 剧烈快速摆动，高危状态 |

---

## 技术栈

| 组件 | 版本 | 方式 |
|------|------|------|
| Docker | 29.6.1 | 容器化运行 |
| Kafka | 7.5.0 | Docker，4 partition |
| InfluxDB | 2.7 | Docker |
| HBase | 2.0.3 (旧镜像) | Docker，已配置 healthcheck |
| Flink | 1.19.3 | Docker（待网络恢复后更新） |
| Python | 3.12+ | Docker (采集层) + 主机脚本 (模拟器) |
| FastAPI | latest | Docker |

---

## 文件结构

```
summer_all1/
├── docker/docker-compose.yml     # 全部服务编排
├── scripts/
│   ├── run_8_patients.py         # 8 患者动态数据模拟器（核心）
│   ├── bridge.py                 # Kafka→InfluxDB+HBase 桥接器（核心）
│   ├── test_dashboard.html       # 趋势折线图仪表盘
│   └── read_hbase.py            # HBase 数据查看
├── data_collector/               # 采集层源码
├── flink_computation/            # Flink 作业（待重建）
├── api_gateway/                  # API 网关
├── docs/
│   ├── hbase-design.md           # HBase 表结构
│   └── layer4-interface-contract.md  # L4 接口契约
└── ARCHITECTURE.md               # 系统架构设计
```

---

## 已知问题

| 问题 | 状态 |
|------|------|
| Docker Hub 镜像源不稳定 | 已配置镜像源 `https://docker.m.daocloud.io` 等 |
| HBase 容器稳定性 | 旧镜像 harisekhon/hbase:2.0 偶发崩溃，需 `docker compose restart hbase` |
| Flink 窗口不触发 | 用 bridge.py 替代，Flink 待后续重建 jar 后恢复 |
| 桥接器无守护进程 | 采集器容器重启后需手动拉起 bridge.py |
