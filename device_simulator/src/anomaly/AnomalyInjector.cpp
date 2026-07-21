#include "anomaly/AnomalyInjector.h"
#include <iostream>

namespace device_sim {

AnomalyInjector::AnomalyInjector(
    asio::io_context& io_context,
    const std::vector<AnomalyConfig>& configs,
    std::vector<std::shared_ptr<DeviceConnection>>& connections)
    : io_context_(io_context)
    , timer_(io_context)
    , connections_(connections)
{
    for (const auto& cfg : configs)
    {
        anomalies_.push_back({cfg, false, {}});
    }
}

void AnomalyInjector::Start()
{
    start_time_ = std::chrono::steady_clock::now();
    timer_.expires_after(std::chrono::milliseconds(kCheckIntervalMs));

    auto self = shared_from_this(); // 但 AnomalyInjector 不是 enable_shared_from_this
    // 用裸指针捕获，生命周期由 Simulator 保证
    timer_.async_wait([this](const asio::error_code& ec)
    {
        if (ec) return;
        CheckAnomalies();
    });
}

void AnomalyInjector::Stop()
{
    timer_.cancel();
}

void AnomalyInjector::CheckAnomalies()
{
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - start_time_).count();

    for (auto& anomaly : anomalies_)
    {
        if (anomaly.active)
        {
            // 检查是否需要恢复
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                now - anomaly.started_at).count();
            if (anomaly.config.params.contains("duration_sec") &&
                duration >= static_cast<int>(anomaly.config.params["duration_sec"]))
            {
                RevertAnomaly(anomaly);
            }
            continue;
        }

        // 检查是否到达触发时间
        if (elapsed >= anomaly.config.trigger_after_sec)
        {
            TriggerAnomaly(anomaly);
        }
    }

    // 继续下一轮检查
    timer_.expires_after(std::chrono::milliseconds(kCheckIntervalMs));
    timer_.async_wait([this](const asio::error_code& ec)
    {
        if (ec) return;
        CheckAnomalies();
    });
}

void AnomalyInjector::TriggerAnomaly(ActiveAnomaly& anomaly)
{
    auto conn = FindConnection(anomaly.config.device_id);
    if (!conn)
    {
        std::cerr << "[ANOMALY] device " << anomaly.config.device_id
                  << " not found, cannot inject." << std::endl;
        return;
    }

    anomaly.active = true;
    anomaly.started_at = std::chrono::steady_clock::now();

    const auto& type = anomaly.config.type;
    std::cout << "[ANOMALY] Injecting " << type
              << " on " << anomaly.config.device_id << std::endl;

    if (type == "hr_bradycardia")
    {
        double target_hr = anomaly.config.params.value("target_hr", 40.0);
        conn->model()->OverrideParam("heart_rate", target_hr);
    }
    else if (type == "hr_tachycardia")
    {
        double target_hr = anomaly.config.params.value("target_hr", 140.0);
        conn->model()->OverrideParam("heart_rate", target_hr);
    }
    else if (type == "spo2_drop")
    {
        double target = anomaly.config.params.value("target_spo2", 85.0);
        conn->model()->OverrideParam("spo2", target);
    }
    else if (type == "hypertension")
    {
        double target = anomaly.config.params.value("target_sbp", 180.0);
        conn->model()->OverrideParam("sys_bp", target);
    }
    else if (type == "fever")
    {
        double target = anomaly.config.params.value("target_temp", 39.5);
        conn->model()->OverrideParam("temperature", target);
    }
    else if (type == "signal_loss")
    {
        // 模拟信号丢失：将 SpO2 设为 0（标记无效）
        conn->model()->OverrideParam("spo2", 0.0);
    }
    else
    {
        std::cerr << "[ANOMALY] Unknown anomaly type: " << type << std::endl;
        anomaly.active = false;
    }
}

void AnomalyInjector::RevertAnomaly(ActiveAnomaly& anomaly)
{
    auto conn = FindConnection(anomaly.config.device_id);
    if (!conn) return;

    std::cout << "[ANOMALY] Reverting " << anomaly.config.type
              << " on " << anomaly.config.device_id << std::endl;

    // 清除所有覆盖值，恢复为正常生成
    if (anomaly.config.type == "hr_bradycardia" || anomaly.config.type == "hr_tachycardia")
    {
        conn->model()->ClearOverride("heart_rate");
    }
    else if (anomaly.config.type == "spo2_drop")
    {
        conn->model()->ClearOverride("spo2");
    }
    else if (anomaly.config.type == "hypertension")
    {
        conn->model()->ClearOverride("sys_bp");
    }
    else if (anomaly.config.type == "fever")
    {
        conn->model()->ClearOverride("temperature");
    }

    anomaly.active = false;
}

std::shared_ptr<DeviceConnection> AnomalyInjector::FindConnection(
    const std::string& device_id)
{
    for (auto& conn : connections_)
    {
        if (conn && conn->device_id() == device_id)
            return conn;
    }
    return nullptr;
}

}  // namespace device_sim
