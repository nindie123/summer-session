# 项目目录结构

> 多语言 Monorepo 架构，三语言共存于同一仓库。

---

## 顶层结构

```
summer_all1/
├── ARCHITECTURE.md               # 系统架构设计（总纲）
├── CODE_STANDARDS.md              # 代码规范
├── PROJECT_STRUCTURE.md           # 本文件 — 目录说明
├── README.md                      # 项目介绍 + 快速开始

├── docs/
│   ├── hbase-design.md            # HBase 表结构设计
│   └── layer4-interface-contract.md # L4 接口契约

├── device_simulator/              # L1: C++ 设备模拟器（待编译）
├── data_collector/                # L2: Python 数据采集层
├── flink_computation/             # L3: Java Flink 计算层（已部署运行）
├── api_gateway/                   # REST API + 大屏可视化 + 仪表盘

├── scripts/                       # 核心运行脚本
│   ├── run_8_patients.py          # ★ 8 患者动态数据模拟器
│   ├── bridge_simple.py           # （备用）Kafka→InfluxDB+HBase 桥接器
│   ├── bigscreen_demo.html        # ★ 大屏可视化 Demo（WebSocket 实时推送）
│   ├── test_dashboard.html        # ★ 实时体征卡片仪表盘
│   ├── read_hbase.py              # HBase 数据可读查看
│   └── read_hbase.bat             # HBase 查看（双击）

├── docker/
│   ├── docker-compose.yml         # 全部服务编排
│   ├── collector.Dockerfile
│   ├── api-gateway.Dockerfile
│   ├── simulator.Dockerfile
│   ├── flink.Dockerfile
│   ├── flink-fix.Dockerfile       # Flink 修复镜像
│   └── hbase.Dockerfile

└── .claude/                       # Claude Code 项目配置
```

---

## L1: device_simulator/ — C++ 设备模拟器

```
device_simulator/
├── CMakeLists.txt                  # 顶层 CMake 构建文件
├── src/
│   ├── main.cpp                    # 入口：加载配置，启动模拟器
│   │
│   ├── core/                       # 核心框架
│   │   ├── Simulator.h/cpp         # 模拟器主控（管理所有设备线程）
│   │   ├── DeviceConnection.h/cpp  # 单设备 TCP 连接管理
│   │   ├── DeviceModel.h/cpp       # 设备模型基类（抽象接口）
│   │   └── Config.h/cpp            # 配置加载（JSON 解析 → 配置对象）
│   │
│   ├── protocol/                   # 通信协议
│   │   └── FrameProtocol.h/cpp     # Length + JSON 帧编码
│   │
│   ├── devices/                    # 具体设备实现
│   │   ├── MonitorModel.h/cpp      # 监护仪（HR, BP, SpO2）
│   │   ├── VentilatorModel.h/cpp   # 呼吸机（RR, TV, PEEP）
│   │   ├── InfusionPumpModel.h/cpp # 输液泵（流速, 累计量）
│   │   └── TempSensorModel.h/cpp   # 体温探头（T1, T2）
│   │
│   └── anomaly/                    # 异常注入
│       └── AnomalyInjector.h/cpp   # 异常场景定义与触发
│
├── config/
│   └── devices.json                # 设备配置文件
│
├── tests/                          # 单元测试
│   ├── CMakeLists.txt
│   ├── test_frame_protocol.cpp
│   ├── test_device_model.cpp
│   └── test_anomaly_injector.cpp
│
└── README.md                       # 构建与运行说明
```

### 构建说明

```bash
cd device_simulator
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
./bin/simulator ../config/devices.json
```

---

## L2: data_collector/ — Python 数据采集层

```
data_collector/
├── pyproject.toml                   # 项目元数据 + 依赖声明
├── src/
│   ├── __init__.py
│   │
│   ├── main.py                      # 应用入口
│   │
│   ├── server/                      # 连接层
│   │   ├── __init__.py
│   │   ├── tcp_server.py            # TCP 接入器 (asyncio.start_server)
│   │   └── conn_manager.py          # 连接管理器
│   │
│   ├── pipeline/                    # 处理管道
│   │   ├── __init__.py
│   │   ├── frame_decoder.py         # 帧解码器 (Length+JSON)
│   │   ├── parser.py                # JSON 解析器
│   │   ├── validator.py             # 数据验证器
│   │   ├── normalizer.py            # 标准化器
│   │   ├── enricher.py              # 元数据富化器
│   │   └── pipeline.py              # 管道编排引擎
│   │
│   ├── kafka/                       # 消息投递
│   │   ├── __init__.py
│   │   ├── producer.py              # Kafka 生产者
│   │   ├── router.py                # 消息路由
│   │   └── dlq.py                   # 死信队列
│   │
│   ├── state/                       # 状态管理
│   │   ├── __init__.py
│   │   ├── patient_binder.py        # 患者-设备绑定
│   │   ├── device_tracker.py        # 设备跟踪
│   │   └── health_checker.py        # 健康检查
│   │
│   ├── models/                      # 数据模型
│   │   ├── __init__.py
│   │   ├── patient_vital_record.py  # PatientVitalRecord
│   │   └── enums.py                 # 枚举定义
│   │
│   └── observability/              # 可观测性
│       ├── __init__.py
│       ├── metrics.py               # Prometheus 指标
│       └── logger.py                # 结构化日志
│
├── tests/                           # 测试
│   ├── __init__.py
│   ├── test_frame_decoder.py
│   ├── test_pipeline.py
│   └── test_patient_binder.py
│
└── README.md
```

### 运行方式

```bash
cd data_collector
pip install -e ".[dev]"
python src/main.py --config config.yaml
```

---

## L3: flink_computation/ — Java Flink 实时计算（已部署运行）

```
flink_computation/
├── pom.xml                           # Maven 构建文件
├── src/
│   ├── main/
│   │   ├── java/com/monitor/
│   │   │   ├── job/                  # Flink Job 入口
│   │   │   │   └── VitalSignProcessingJob.java
│   │   │   │
│   │   │   ├── function/             # Flink 算子函数
│   │   │   │   ├── VitalSignDeserializer.java   # Kafka 反序列化
│   │   │   │   ├── DeviceFusionFunction.java     # 多设备融合
│   │   │   │   ├── MewsCalculator.java           # MEWS 评分
│   │   │   │   ├── TrendAnalyzer.java            # 趋势分析（未使用）
│   │   │   │   ├── RiskClassifier.java           # 风险分级（未使用）
│   │   │   │   └── AlertDeduplicator.java        # 告警去重
│   │   │   │
│   │   │   ├── model/                # Java POJO 模型
│   │   │   │   ├── PatientVitalRecord.java
│   │   │   │   ├── PatientSnapshot.java
│   │   │   │   ├── MewsScore.java
│   │   │   │   ├── TrendResult.java
│   │   │   │   ├── DiagnosticInput.java
│   │   │   │   └── AlertEvent.java
│   │   │   │
│   │   │   └── sink/                 # 输出 Sink
│   │   │       ├── DiagnosticInputKafkaSink.java
│   │   │       ├── AlertEventKafkaSink.java
│   │   │       ├── InfluxDbSink.java
│   │   │       └── HBaseSink.java
│   │   │
│   │   └── resources/
│   │       ├── application.properties # Flink 配置
│   │       └── log4j.properties       # 日志配置
│   │
│   └── test/java/com/monitor/
│       ├── function/
│       │   ├── MewsCalculatorTest.java
│       │   └── TrendAnalyzerTest.java
│       └── model/
│           └── PatientSnapshotTest.java
│
└── README.md
```

### 构建与提交

```bash
cd flink_computation
mvn clean package -DskipTests
# 构建 Docker 镜像
docker build -t docker-flink-job:latest -f docker/flink.Dockerfile .
# 提交作业（docker-compose 自动执行）
docker compose up -d flink-job
```

**注意：** 实际部署使用 `docker-flink-job:fixed` 镜像（基于 `docker-flink-job:latest` 修复 CRLF 换行问题）。

**当前状态：** ✅ 已部署运行，全部算子 RUNNING。

---

## api_gateway/ — Python REST 查询服务 + 大屏可视化

```
api_gateway/
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── main.py                       # FastAPI 应用入口
│   ├── routers/                      # 路由
│   │   ├── __init__.py
│   │   ├── vitals.py                 # 体征查询
│   │   ├── mews.py                   # MEWS 查询
│   │   ├── alerts.py                 # 告警查询
│   │   ├── wards.py                  # 病区概览
│   │   └── bigscreen.py              # ★ 大屏可视化接口
│   ├── clients/                      # 数据源客户端
│   │   ├── __init__.py
│   │   └── influx_client.py          # InfluxDB 查询客户端
│   └── models/                       # API 响应模型
│       ├── __init__.py
│       └── schemas.py
├── tests/
└── README.md
```

### 大屏可视化

| 端点 | 说明 |
|------|------|
| `GET /api/v1/bigscreen/overview` | REST 聚合：全部患者体征 + MEWS + 趋势 |
| `WS /api/v1/bigscreen/ws` | WebSocket 实时推送（3s 广播） |
| `GET /bigscreen` | 暗色主题大屏 Demo 页面 |
| `GET /test` | 8 患者体征卡片面板 |

### 运行方式

```bash
cd api_gateway
pip install -e ".[dev]"
uvicorn src.main:app --host 0.0.0.0 --port 8000
```

---

## docker/ — 基础设施

```
docker/
├── docker-compose.yml                # 主编排文件
├── collector.Dockerfile              # 采集层镜像
├── api-gateway.Dockerfile            # API 网关镜像
├── simulator.Dockerfile              # 模拟器镜像
├── flink.Dockerfile                  # Flink + Maven 构建镜像
├── flink-fix.Dockerfile              # Flink 修复镜像（修正 CRLF 换行）
├── hbase.Dockerfile                  # HBase 构建镜像
├── flink-submit.sh                   # Flink 作业提交脚本
└── .env                              # 环境变量
```

---

## scripts/ — 开发脚本

```
scripts/
├── run_8_patients.py                 # ★ 8 患者动态数据模拟器（主力）
├── bridge_simple.py                  # （备用）桥接器 Kafka→InfluxDB+HBase
├── bridge.py                         # （备用）旧版桥接器
├── bigscreen_demo.html               # ★ 大屏可视化 Demo 页面
├── test_dashboard.html               # ★ 体征卡片仪表盘
├── read_hbase.py                     # HBase 数据查看
├── read_hbase.bat                    # HBase 查看（双击运行）
└── run_50_patients.py                # 50 患者模拟器（旧）
```

---

## 三语言目录对比速查

| 模块 | 语言 | 构建系统 | 入口文件 | 端口 |
|------|------|---------|---------|------|
| `device_simulator/` | C++20 | CMake | `src/main.cpp` | 无（连接 collector:9001） |
| `data_collector/` | Python 3.12+ | pyproject.toml | `src/main.py` | 9001(TCP) |
| `flink_computation/` | Java 17 | Maven | `src/.../VitalSignProcessingJob.java` | 无（Flink 框架分配） |
| `api_gateway/` | Python 3.12+ | pyproject.toml | `src/main.py` | 8000(HTTP) |

---

## 文件命名规范

| 语言 | 文件名 | 示例 |
|------|--------|------|
| C++ | `PascalCase.h/cpp` | `DeviceConnection.h`, `MewsCalculator.cpp` |
| Python | `snake_case.py` | `frame_decoder.py`, `patient_binder.py` |
| Java | `PascalCase.java` | `PatientSnapshot.java`, `MewsCalculator.java` |
| 配置文件 | `kebab-case` | `devices.json`, `docker-compose.yml` |
| 文档 | `UPPER_SNAKE_CASE.md` | `ARCHITECTURE.md`, `CODE_STANDARDS.md` |
