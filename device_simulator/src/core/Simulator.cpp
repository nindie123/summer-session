#include "core/Simulator.h"
#include "devices/MonitorModel.h"
#include "devices/TempSensorModel.h"

#include <iostream>
#include <csignal>

namespace device_sim {

Simulator::Simulator(Config config)
    : config_(std::move(config))
    , signals_(io_context_, SIGINT, SIGTERM)
{
    CreateDevices();
}

Simulator::~Simulator()
{
    Stop();
}

void Simulator::Run()
{
    std::cout << "[SIMULATOR] Starting with "
              << connections_.size() << " devices" << std::endl;

    // 启动所有设备连接
    for (auto& conn : connections_)
    {
        conn->Start();
    }

    // 启动异常注入
    if (anomaly_injector_)
    {
        anomaly_injector_->Start();
    }

    // 注册信号处理
    signals_.async_wait([this](const asio::error_code&, int sig)
    {
        std::cout << "[SIMULATOR] Received signal " << sig
                  << ", shutting down..." << std::endl;
        Stop();
    });

    // 运行事件循环
    io_context_.run();

    std::cout << "[SIMULATOR] Shutdown complete." << std::endl;
}

void Simulator::Stop()
{
    if (anomaly_injector_)
    {
        anomaly_injector_->Stop();
    }
    for (auto& conn : connections_)
    {
        conn->Stop();
    }
    io_context_.stop();
}

void Simulator::CreateDevices()
{
    for (const auto& dev_cfg : config_.devices())
    {
        std::unique_ptr<DeviceModel> model;

        if (dev_cfg.device_type == "Monitor")
        {
            model = std::make_unique<MonitorModel>(dev_cfg);
        }
        else if (dev_cfg.device_type == "TempSensor")
        {
            model = std::make_unique<TempSensorModel>(dev_cfg);
        }
        else
        {
            std::cerr << "[SIMULATOR] Unknown device type: "
                      << dev_cfg.device_type << std::endl;
            continue;
        }

        auto conn = std::make_shared<DeviceConnection>(
            io_context_,
            std::move(model),
            config_.collector().host,
            static_cast<uint16_t>(config_.collector().port),
            "sim_secret_2024"
        );

        conn->SetErrorCallback(
            [this](const std::string& dev_id, const std::string& err)
            {
                OnDeviceError(dev_id, err);
            });

        connections_.push_back(std::move(conn));
    }

    // 创建异常注入器
    if (!config_.anomalies().empty())
    {
        anomaly_injector_ = std::make_unique<AnomalyInjector>(
            io_context_, config_.anomalies(), connections_);
    }
}

void Simulator::OnDeviceError(const std::string& device_id, const std::string& error)
{
    std::cerr << "[SIMULATOR] Device " << device_id
              << " error: " << error << std::endl;
}

}  // namespace device_sim
