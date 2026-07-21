#ifndef SIMULATOR_ANOMALY_INJECTOR_H_
#define SIMULATOR_ANOMALY_INJECTOR_H_

#include <string>
#include <vector>
#include <memory>
#include <chrono>

#include <asio.hpp>

#include "core/Config.h"
#include "core/DeviceConnection.h"

namespace device_sim {

/// 异常注入引擎 — 按配置定时触发异常场景
class AnomalyInjector {
public:
    AnomalyInjector(
        asio::io_context& io_context,
        const std::vector<AnomalyConfig>& configs,
        std::vector<std::shared_ptr<DeviceConnection>>& connections);

    /// 启动异常定时器
    void Start();

    /// 停止
    void Stop();

private:
    struct ActiveAnomaly {
        AnomalyConfig config;
        bool active{false};
        std::chrono::steady_clock::time_point started_at;
    };

    void CheckAnomalies();
    void TriggerAnomaly(ActiveAnomaly& anomaly);
    void RevertAnomaly(ActiveAnomaly& anomaly);

    /// 根据 device_id 查找连接
    std::shared_ptr<DeviceConnection> FindConnection(const std::string& device_id);

    asio::io_context& io_context_;
    asio::steady_timer timer_;
    std::vector<ActiveAnomaly> anomalies_;
    std::vector<std::shared_ptr<DeviceConnection>>& connections_;

    // 检查间隔
    static constexpr int kCheckIntervalMs = 1000;

    // 启动时间
    std::chrono::steady_clock::time_point start_time_;
};

}  // namespace device_sim

#endif  // SIMULATOR_ANOMALY_INJECTOR_H_
