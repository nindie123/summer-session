# 代码规范

> 适用于本项目三语言（C++ / Python / Java）的统一编码约定。
> 目标：代码可读、可维护、可审查。

---

## 目录

1. [通用原则](#一通用原则)
2. [C++ 规范 (设备模拟器)](#二c-规范-设备模拟器)
3. [Python 规范 (采集层 + API 网关)](#三python-规范-采集层--api-网关)
4. [Java 规范 (Flink 计算层)](#四java-规范-flink-计算层)
5. [Git 提交规范](#五git-提交规范)
6. [日志规范](#六日志规范)
7. [错误处理规范](#七错误处理规范)
8. [代码审查清单](#八代码审查清单)

---

## 一、通用原则

### 1.1 核心准则

1. **可读性优先** — 代码被读的次数远多于写的次数
2. **一致性** — 宁可坚持一个不完美的风格，也不要混用多种风格
3. **显式优于隐式** — 清晰的意图比"聪明"的技巧更重要
4. **最小惊讶原则** — 接口设计应符合直觉，不让人意外
5. **注释讲 Why，代码讲 What** — 注释解释"为什么这样做"，代码本身表达"做了什么"

### 1.2 全链路追踪规范

所有层必须传递 `traceId`：

```
数据产生时生成 traceId → 贯穿 L2 → L3 → L4 全程

格式: "trace_" + UUID 的 hex 前 24 位
示例: "trace_a1b2c3d4e5f6a7b8c9d0e1f"

传递方式:
  - L1: 不生成 traceId（由收集层负责）
  - L2: 在 Enricher 阶段生成 traceId，追加到消息
  - L3: 透传 traceId，在输出中保留
  - 日志: 每条日志必须带 traceId
  - 异常: 异常链中传递 traceId
```

### 1.3 日志格式规范（跨语言统一）

```
输出格式: JSON（便于日志中心收集和分析）

标准字段:
  {
    "timestamp": "2026-07-21T10:00:00.000Z",
    "level": "INFO",
    "logger": "module_name",
    "traceId": "trace_...",
    "message": "human readable message",
    "context": {                   // 结构化上下文
      "patientId": "P0001",
      "deviceId": "monitor_001",
      "processingMs": 42
    },
    "exception": null              // 异常时填入
  }
```

---

## 二、C++ 规范 (设备模拟器)

### 2.1 语言标准

- **C++20**
- 编译器：GCC 12+ / Clang 16+ / MSVC 2022+
- 禁止使用：C 风格宏（除 include guard）、C 风格类型转换、`new`/`delete`（使用 RAII + 智能指针）

### 2.2 命名规范

| 类别 | 风格 | 示例 | 反例 |
|------|------|------|------|
| 类/结构体 | PascalCase | `class DeviceConnection` | `device_connection` |
| 接口/抽象类 | I + PascalCase | `class IDeviceModel` | `DeviceModelInterface` |
| 成员函数 | PascalCase | `Connect()`, `SendData()` | `connect()`, `send_data()` |
| 成员变量 | m_ + camelCase | `m_deviceId`, `m_connectedAt` | `deviceId_`, `m_device_id` |
| 局部变量 | camelCase | `deviceId`, `payloadLen` | `device_id`, `DeviceId` |
| 常量/枚举值 | k + PascalCase | `kMaxRetries`, `kDefaultPort` | `MAX_RETRIES` |
| 命名空间 | snake_case | `namespace device_sim` | `DeviceSim` |
| 宏 (少用) | UPPER_SNAKE_CASE | `LOG_DEBUG(msg)` | `log_debug(msg)` |
| 文件命名 | PascalCase | `DeviceModel.h`, `TcpServer.cpp` | `device_model.h` |

### 2.3 头文件规范

```cpp
// 标准头文件保护
#ifndef MONITOR_DEVICE_MODEL_H_
#define MONITOR_DEVICE_MODEL_H_

#include <string>
#include <vector>

#include "core/DeviceModel.h"  // 本项目头文件用引号

namespace device_sim {

class MonitorModel final : public DeviceModel {
 public:
  explicit MonitorModel(std::string deviceId);

  // 禁止拷贝
  MonitorModel(const MonitorModel&) = delete;
  MonitorModel& operator=(const MonitorModel&) = delete;

  // 允许移动
  MonitorModel(MonitorModel&&) = default;
  MonitorModel& operator=(MonitorModel&&) = default;

  ~MonitorModel() override = default;

  // —— 接口实现 ——
  [[nodiscard]] std::vector<Observation> GenerateData() override;
  [[nodiscard]] std::string DeviceType() const override { return "Monitor"; }

 private:
  std::string m_deviceId;
  std::mt19937 m_rng;  // 随机数生成器
};

}  // namespace device_sim

#endif  // MONITOR_DEVICE_MODEL_H_
```

### 2.4 代码风格

- **缩进：** 2 空格
- **行宽：** 100 字符
- **花括号：** Allman 风格（左括号换行）

```cpp
// ✅ Allman 风格
bool Connect(const std::string& host, uint16_t port)
{
    if (host.empty())
    {
        LOG_ERROR("host is empty");
        return false;
    }
    m_socket.connect(host, port);
    return true;
}

// ❌ 不允许：K&R 风格
bool Connect(const std::string& host, uint16_t port) {
    // ...
}
```

### 2.5 智能指针使用

```cpp
// 所有权明确：唯一所有权
auto conn = std::make_unique<DeviceConnection>(deviceId, host, port);

// 共享所有权（少用，优先设计为唯一所有权）
auto model = std::make_shared<MonitorModel>("monitor_001");

// 非拥有引用：裸指针
void ProcessDevice(DeviceModel* model);  // 表示"可能为空"
void ProcessDevice(DeviceModel& model);  // 表示"一定不为空"
```

### 2.6 错误处理

- 返回值的错误用 `std::expected`（C++23）或 `optional`（C++17）
- 不可恢复错误用异常
- 设备模拟器是测试工具，不做复杂的错误恢复，出错时日志 + 退出

```cpp
// ✅ 用 optional 表示可能失败的操作
[[nodiscard]] std::optional<Observation> GetLatestObservation(const std::string& code);

// ❌ 不用的方式
Observation GetLatestObservation(const std::string& code);  // 调用者不知道可能失败
```

### 2.7 格式化工具

```
.clang-format 基于 Google 风格，调整：
  - IndentWidth: 2
  - ColumnLimit: 100
  - BreakBeforeBraces: Allman
```

---

## 三、Python 规范 (采集层 + API 网关)

### 3.1 版本要求

- **Python 3.12+**
- 依赖管理：`pyproject.toml` + `uv` 或 `pip`

### 3.2 命名规范

| 类别 | 风格 | 示例 | 反例 |
|------|------|------|------|
| 模块/包 | snake_case | `frame_decoder.py` | `frameDecoder.py` |
| 类 | PascalCase | `class TcpServer` | `class tcp_server` |
| 函数/方法 | snake_case | `def process_message()` | `def processMessage()` |
| 变量 | snake_case | `device_id = ""` | `deviceId = ""` |
| 常量 | UPPER_SNAKE_CASE | `MAX_CONNECTIONS = 1000` | `maxConnections = 1000` |
| 私有成员 | _ 前缀 | `self._conn_registry` | `self.connRegistry` |
| 类型变量 | PascalCase | `type ObservationDict = dict` | `type obs_dict = dict` |

### 3.3 类型注解

函数签名必须包含类型注解，这是**强制要求**：

```python
# ✅ 完整类型注解
from collections.abc import Sequence
from dataclasses import dataclass

@dataclass
class ValidationResult:
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

def validate_observations(
    observations: list[Observation],
    patient_id: str | None = None,
) -> ValidationResult:
    """验证体征观测值。

    Args:
        observations: 待验证的观测值列表
        patient_id: 患者 ID（可选，用于日志追踪）

    Returns:
        ValidationResult: 验证结果
    """
    result = ValidationResult(passed=True)
    for obs in observations:
        if obs.value < 0 or obs.value > 300:
            result.passed = False
            result.errors.append(f"HR value {obs.value} out of range [0, 300]")
    return result


# ❌ 不允许缺少类型注解
def validate_observations(observations, patient_id=None):
    # 没有类型信息，调用者不知道传入什么
    pass
```

### 3.4 异步编程规范

```python
# ✅ 异步函数用 async/await
async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            result = await pipeline.process(data)
            await producer.send(result)
    except ConnectionResetError:
        logger.warning("connection reset", extra={"traceId": trace_id})
    finally:
        writer.close()
        await writer.wait_closed()

# ❌ 禁止在异步代码中调用阻塞函数（如有必要请用 run_in_executor）
def blocking_io():  # 不要直接在 async 函数里调用
    time.sleep(1)  # 阻塞事件循环！
```

### 3.5 导入顺序

```python
# 标准库
import json
import logging
from collections.abc import Sequence
from datetime import datetime, timezone

# 三方库
import orjson
from aiokafka import AIOKafkaProducer

# 本项目
from src.models.patient_vital_record import PatientVitalRecord
from src.pipeline.frame_decoder import FrameDecoder

# 每个分类之间空一行，同一分类内按字母序排列
```

### 3.6 格式化工具

- 格式化：`ruff format`（行宽 100）
- Linter：`ruff check`（选择所有规则，逐条审视是否启用）
- 类型检查：`pyright` 或 `mypy`（严格模式）

```toml
# pyproject.toml 配置
[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["ALL"]

[tool.pyright]
typeCheckingMode = "strict"
```

### 3.7 数据类约定

项目中的数据模型统一使用 `dataclass`：

```python
from dataclasses import dataclass, field

@dataclass
class PatientVitalRecord:
    """统一体征数据模型。"""
    schema_version: str = "2.0"
    message_id: str = ""
    trace_id: str = ""
    observations: list[Observation] = field(default_factory=list)
    # 注意：可变默认值必须用 field(default_factory=)
```

---

## 四、Java 规范 (Flink 计算层)

### 4.1 版本要求

- **Java 17**
- Flink 1.19
- Maven 3.9+

### 4.2 命名规范

| 类别 | 风格 | 示例 | 反例 |
|------|------|------|------|
| 类/接口 | PascalCase | `class MewsCalculator` | `class mewsCalculator` |
| 方法 | camelCase | `calculateMewsScore()` | `calculate_mews_score()` |
| 变量 | camelCase | `PatientSnapshot snapshot` | `PatientSnapshot snapshot_data` |
| 常量 | UPPER_SNAKE_CASE | `MAX_MEWS_SCORE = 14` | `maxMewsScore = 14` |
| 包名 | 全小写 | `com.monitor.function` | `com.monitor.Function` |
| 枚举 | PascalCase | `enum RiskLevel` | `enum riskLevel` |
| 枚举值 | UPPER_SNAKE_CASE | `STABLE, WARNING` | `Stable, Warning` |

### 4.3 代码风格

- **缩进：** 4 空格（Java 标准）
- **行宽：** 120 字符
- **花括号：** K&R 风格（左括号不换行）— Java 标准

### 4.4 Flink 特有规范

```java
// ✅ 所有 Flink 函数类必须可序列化（实现 Serializable）
public class MewsCalculator implements Serializable {
    private static final long serialVersionUID = 1L;

    // 非序列化字段必须标记 transient
    private transient Configuration config;
}
```

### 4.5 Lombok 使用

- 可用 Lombok 减少样板代码
- POJO 类使用 `@Data`、`@Builder`、`@NoArgsConstructor`、`@AllArgsConstructor`

```java
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatientSnapshot {
    private String patientId;
    private Map<String, VitalSign> vitals;
    private List<String> activeDevices;
    private String timestamp;
}
```

### 4.6 格式化工具

- **格式化：** `spotless:check` / `spotless:apply`（Google Java Format）
- **Linter：** PMD 或 Error Prone
- **配置在 pom.xml 中**

```xml
<plugin>
    <groupId>com.diffplug.spotless</groupId>
    <artifactId>spotless-maven-plugin</artifactId>
    <configuration>
        <java>
            <googleJavaFormat/>
        </java>
    </configuration>
</plugin>
```

---

## 五、Git 提交规范

### 5.1 分支策略

```
main        ← 稳定版本，只能通过 PR 合并
  ├── feat/*    ← 功能分支（如 feat/patient-binder）
  ├── fix/*     ← 修复分支
  ├── refactor/*← 重构分支
  └── docs/*    ← 文档分支
```

### 5.2 提交信息格式

```
<type>(<scope>): <简短描述>

<可选的详细描述>

<可选的关闭 issue>
```

**type：**

| Type | 含义 |
|------|------|
| feat | 新功能 |
| fix | 修复 bug |
| docs | 文档变更 |
| style | 代码格式（不影响功能） |
| refactor | 重构 |
| test | 测试相关 |
| chore | 构建/工具/依赖变更 |

**scope：**

| Scope | 含义 |
|-------|------|
| sim | device_simulator |
| collector | data_collector |
| flink | flink_computation |
| api | api_gateway |
| docker | docker 配置 |
| docs | 文档 |

**示例：**

```
feat(sim): 实现监护仪数据生成器

- 支持 HR/BP/SpO2/RR 参数生成
- 符合正常生理范围的随机波动
- 可通过配置注入心率异常

Closes #12
```

```
fix(collector): 修复 TCP 粘包处理中长度字段字节序错误

Length 头应为 Big-Endian，之前错误实现为 Little-Endian
导致跨平台采集数据解析异常。
```

### 5.3 PR 规范

- PR 标题同 commit message 格式
- PR 描述包含：改动了什么、为什么改、如何测试
- 至少 1 人 Review 后方可合并
- 合并方式：Squash Merge

---

## 六、日志规范

### 6.1 日志级别使用规则

| 级别 | 使用场景 | 示例 |
|------|---------|------|
| ERROR | 影响功能的异常（需要人工介入） | 数据库连接失败、Kafka 不可用 |
| WARN | 潜在问题但不影响功能 | 数据验证告警、重连重试 |
| INFO | 重要的生命周期事件 | 服务启动、连接建立、配置加载 |
| DEBUG | 调试信息（默认关闭） | 消息处理流水线步骤 |
| TRACE | 详细追踪（仅开发用） | 帧解码细节 |

### 6.2 结构化字段

```python
# Python 示例
logger.info(
    "Message processed successfully",
    extra={
        "traceId": trace_id,
        "patientId": patient_id,
        "deviceId": device_id,
        "processingMs": elapsed_ms,
        "observationsCount": len(observations),
    },
)
```

---

## 七、错误处理规范

### 7.1 原则

1. **fail fast** — 错误尽早暴露，不吞异常
2. **区分技术异常和业务异常** — 技术异常（网络、DB）走系统级处理，业务异常（数据验证失败）走业务逻辑
3. **异常的 traceId 贯穿** — 每层在捕获异常时携带 traceId
4. **死信队列兜底** — 处理失败的消息进入 DLQ，不阻塞主流程

### 7.2 各层错误处理策略

| 层 | 策略 |
|----|------|
| L1 模拟器 | 连接失败则重试（指数退避），重试耗尽则退出进程打印错误 |
| L2 采集层 | 设备数据解析失败 → DLQ，不阻塞管道；Kafka 发送失败 → 本地缓冲 + 重试 |
| L3 Flink | Checkpoint 容错，算子异常进入侧输出流，不破坏 Job |

---

## 八、代码审查清单

### 每次 PR Review 检查以下要点：

| # | 检查项 | 说明 |
|---|--------|------|
| 1 | **功能正确** | 代码实现了需求吗？有测试覆盖吗？ |
| 2 | **边界条件** | 空值、越界值、并发访问、断开连接等场景处理了吗？ |
| 3 | **日志完整** | 关键路径有日志吗？日志包含 traceId 吗？ |
| 4 | **异常处理** | 异常被吞了吗？处理失败的消息有兜底吗？ |
| 5 | **可读性** | 命名是否清晰？是否需要更多注释（解释 Why）？ |
| 6 | **性能** | 有循环中的同步 IO 吗？大对象分配在热路径上吗？ |
| 7 | **安全** | 有硬编码密钥/IP 吗？输入有校验吗？ |
| 8 | **与架构一致** | 改动符合架构分层吗？接口契约兼容吗？ |
| 9 | **文档同步** | 需要更新文档吗？（接口变更、配置项新增） |
