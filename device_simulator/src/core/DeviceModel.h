#ifndef SIMULATOR_DEVICE_MODEL_H_
#define SIMULATOR_DEVICE_MODEL_H_

#include <string>
#include <vector>
#include <random>
#include <unordered_map>

#include "core/Config.h"

namespace device_sim {

/// 单条观测值
struct Observation {
    std::string code;   // LOINC code
    std::string name;
    double value;
    std::string unit;
};

/// 设备模型基类 — 所有设备实现此接口
class DeviceModel {
public:
    explicit DeviceModel(DeviceConfig config);
    virtual ~DeviceModel() = default;

    DeviceModel(const DeviceModel&) = delete;
    DeviceModel& operator=(const DeviceModel&) = delete;
    DeviceModel(DeviceModel&&) = default;
    DeviceModel& operator=(DeviceModel&&) = default;

    /// 生成一组体征数据
    virtual std::vector<Observation> GenerateData() = 0;

    /// 设备类型标识
    virtual std::string DeviceType() const = 0;

    /// 注入异常值（由 AnomalyInjector 调用）
    void OverrideParam(const std::string& param, double value);

    /// 清除异常覆盖
    void ClearOverride(const std::string& param);

    /// 获取当前参数值（用于外部读取状态）
    double CurrentValue(const std::string& param) const;

    const DeviceConfig& config() const { return config_; }
    const std::string& device_id() const { return config_.device_id; }
    int interval_ms() const { return config_.interval_ms; }

protected:
    /// 在 [min, max] 范围内生成一个带微小波动的随机值
    double GenerateVital(const ParamRange& range, double prev_value);

    DeviceConfig config_;

    // 随机数引擎
    std::mt19937 rng_{std::random_device{}()};

    // 各参数当前值
    std::unordered_map<std::string, double> current_values_;

    // 被 AnomalyInjector 覆盖的值
    std::unordered_map<std::string, double> overrides_;
};

}  // namespace device_sim

#endif  // SIMULATOR_DEVICE_MODEL_H_
