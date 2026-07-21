<<<<<<< HEAD
# summer-session
小学期项目协作
## 项目预期架构
### 系统架构

整体分层流程：**临床设备层 → 数据采集标准化层 → 实时计算异常检测层 → LLM Agent 智能诊断层 → 可视化交互层**

#### 1. 临床设备层
系统原始数据来源，接入全院床边监护设备
- 设备：监护仪、呼吸机、输液泵、血气分析仪、体温探头
- 传输协议：HL7、DICOM、设备私有协议

#### 2. 数据采集与标准化层
统一治理多源异构医疗设备数据
- 多协议适配解析
- 患者与设备绑定（床号、腕带、EMR 关联）
- 数据脱敏、合规处理
- Kafka 实时消息队列数据分发

#### 3. 实时计算与异常检测层
大数据实时流式分析与风险计算
- 基于 Flink 实现体征趋势、变化率、均值实时计算
- 自动计算 MEWS/NEWS/PEWS 危重症评分
- 基线偏离、生理模式异常检测
- 时序数据存储：HBase 实时存储、HDFS 归档存储

#### 4. LLM Agent 智能诊断层
系统 AI 推理核心，实现临床级综合研判
##### 数据支撑
- RAG 医疗知识库：疾病知识、药品信息、设备参数阈值
- 临床指南库：诊疗规范、急救流程、护理常规
- 患者历史库：既往病史、过敏史、检验记录

### 核心能力
- 多参数趋势综合解读（摒弃单点阈值报警）
- 病情分级：稳定 / 预警 / 危急 / 濒死
- 智能鉴别诊断、检查与治疗建议输出
- 病情恶化预测性预警
- 所有 AI 结论支持临床指南溯源

#### 5. 可视化与交互层
多终端临床应用入口
- 护士站大屏：病区患者风险热力图展示
- 医生工作站：体征时间轴 + AI 诊断建议
- 移动端 APP：危急值推送、语音播报、一键处置
- 管理驾驶舱：科室质控、告警响应效率统计

#### 架构优势
- 分层解耦，模块清晰，易于迭代扩展
- 全链路实时流式数据处理
- LLM 多参数融合研判，大幅降低传统监护误报率
- 智能结果可溯源，符合医疗合规要求



=======
# summer-session — 医院实时生命体征监护系统

> 大三小学期项目 · 全容器化一键部署  
> 范围：L1 (设备模拟器) → L2 (数据采集) → L3 (实时计算) + L4 接口契约

---

## 快速开始（一键启动）

### Windows 一键启动

双击 `scripts/start.bat`，或命令行运行：

```bash
cd D:\little\summer_all1
scripts\start.bat
```

### PowerShell

```powershell
.\scripts\run.ps1
```

### 手工启动

```bash
cd docker
docker compose up -d
```

等 20 秒后发测试数据：

```bash
docker compose run --rm device-simulator
```

### 停止

```bash
scripts\stop.bat
# 或
cd docker && docker compose down -v
```

---

## 访问入口

| 入口 | 地址 | 说明 |
|------|------|------|
| API 文档 | http://localhost:8000/docs | 交互式测试所有接口 |
| 病区概览 | http://localhost:8000/api/v1/wards/ICU-EAST/overview | 所有患者状态 |
| P0001 体征 | http://localhost:8000/api/v1/patients/P0001/vitals | 历史体征数据 |
| P0001 MEWS | http://localhost:8000/api/v1/patients/P0001/mews | 评分历史 |
| Flink 监控 | http://localhost:8081 | 实时计算状态 |

---

## 系统架构

```
设备模拟器(L1) ──TCP:9001──→ 采集层(L2) ──Kafka──→ Flink(L3) ──Kafka──→ AI诊断(L4)
   C++                       Python                  Java              留给同学
   2种设备                   管道管线                 MEWS+趋势
   Monitor 1s/次             数据验证                 风险分级
   Temp    30s/次            标准化                  告警检测
                                                    └──→ InfluxDB ──→ API查询(:8000)
```

## 项目结构

```
summer_all1/
├── scripts/              # 一键启动脚本
│   ├── start.bat         ← 双击运行
│   ├── run.ps1           ← PowerShell 运行
│   └── stop.bat          ← 停止服务
├── device_simulator/     # L1: C++ 设备模拟器
├── data_collector/       # L2: Python 数据采集层
├── flink_computation/    # L3: Java Flink 实时计算
├── api_gateway/          # L3附属: REST 查询服务
├── docker/               # Docker Compose 编排
├── docs/                 # 设计文档
├── ARCHITECTURE.md       # 架构设计
├── CODE_STANDARDS.md     # 代码规范
└── PROJECT_STRUCTURE.md  # 目录结构
```

## 数据流向

| 从 → 到 | 协议 | 内容 |
|---------|------|------|
| 设备 → 采集层 | TCP Length+JSON | 心率、血压、血氧、呼吸、体温 |
| 采集层 → Kafka | Topic: `standardized.vitals` | 标准化后的体征数据 |
| Flink → Kafka | Topic: `ai.diagnostic.input` | MEWS 评分 + 趋势 + 风险等级 |
| Flink → Kafka | Topic: `ai.alerts` | 风险 ≥ WARNING 时触发 |
| Flink → InfluxDB | HTTP Line Protocol | 体征 + MEWS 时序存储（API 查询用） |
| **Flink → HBase** ⭐ | **Java HBase Client** | **体征+MEWS综合表（AI 读取用）** |
| API ← InfluxDB | HTTP Flux Query | REST 查询接口 |
| **AI ← HBase** ⭐ | **Thrift (9090)** | **批量历史数据分析** |

## 技术栈

| 层 | 语言/框架 | 运行方式 |
|----|----------|---------|
| L1 模拟器 | C++20 + ASIO | Docker / 原生（需编译） |
| L2 采集层 | Python 3.12+ asyncio | Docker |
| L3 实时计算 | Java 17 + Flink 1.19 | Docker |
| L3 API | Python 3.12+ FastAPI | Docker |
| 消息队列 | Kafka 7.6 | Docker |
| 时序数据库 | InfluxDB 2.7 | Docker |
| **AI 数据存储** ⭐ | **HBase 2.5.6** | **Docker** |
>>>>>>> master
