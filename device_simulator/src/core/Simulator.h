#ifndef SIMULATOR_SIMULATOR_H_
#define SIMULATOR_SIMULATOR_H_

#include <memory>
#include <vector>
#include <string>

#include <asio.hpp>

#include "core/Config.h"
#include "core/DeviceConnection.h"
#include "anomaly/AnomalyInjector.h"

namespace device_sim {

/// 模拟器主控 — 管理所有设备连接和异常注入
class Simulator {
public:
    explicit Simulator(Config config);
    ~Simulator();

    Simulator(const Simulator&) = delete;
    Simulator& operator=(const Simulator&) = delete;

    /// 启动模拟器（阻塞，直到收到 SIGINT 或所有设备断开）
    void Run();

    /// 优雅停止
    void Stop();

private:
    void CreateDevices();
    void OnDeviceError(const std::string& device_id, const std::string& error);

    Config config_;
    asio::io_context io_context_;
    asio::signal_set signals_;

    std::vector<std::shared_ptr<DeviceConnection>> connections_;
    std::unique_ptr<AnomalyInjector> anomaly_injector_;
};

}  // namespace device_sim

#endif  // SIMULATOR_SIMULATOR_H_
