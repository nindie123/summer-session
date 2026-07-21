#ifndef SIMULATOR_CONFIG_H_
#define SIMULATOR_CONFIG_H_

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <chrono>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace device_sim {

/// 单个参数的配置范围
struct ParamRange {
    double min_val;
    double max_val;
    double initial;
};

/// 异常注入配置
struct AnomalyConfig {
    std::string device_id;
    std::string type;           // hr_bradycardia, spo2_drop, signal_loss, etc.
    int trigger_after_sec;      // 启动后多少秒触发
    std::unordered_map<std::string, double> params;
};

/// 采集器连接配置
struct CollectorConfig {
    std::string host;
    int port;
    int reconnect_interval_ms;
    int max_reconnect_attempts;
};

/// 单个设备配置
struct DeviceConfig {
    std::string device_id;
    std::string device_type;   // Monitor or TempSensor
    std::string patient_id;
    int interval_ms;
    std::unordered_map<std::string, ParamRange> params;
};

/// 全局配置
class Config {
public:
    static Config LoadFromFile(const std::string& path);

    const CollectorConfig& collector() const { return collector_; }
    const std::vector<DeviceConfig>& devices() const { return devices_; }
    const std::vector<AnomalyConfig>& anomalies() const { return anomalies_; }

private:
    CollectorConfig collector_;
    std::vector<DeviceConfig> devices_;
    std::vector<AnomalyConfig> anomalies_;
};

}  // namespace device_sim

#endif  // SIMULATOR_CONFIG_H_
