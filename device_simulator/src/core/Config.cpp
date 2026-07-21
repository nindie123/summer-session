#include "core/Config.h"
#include <fstream>
#include <stdexcept>

namespace device_sim {

Config Config::LoadFromFile(const std::string& path)
{
    std::ifstream file(path);
    if (!file.is_open())
    {
        throw std::runtime_error("Cannot open config file: " + path);
    }

    json j;
    file >> j;
    file.close();

    Config cfg;

    // 采集器连接
    auto& collector = j["collector"];
    cfg.collector_.host = collector["host"].get<std::string>();
    cfg.collector_.port = collector["port"].get<int>();
    cfg.collector_.reconnect_interval_ms = collector.value("reconnect_interval_ms", 3000);
    cfg.collector_.max_reconnect_attempts = collector.value("max_reconnect_attempts", 5);

    // 设备列表
    for (auto& dev : j["devices"])
    {
        DeviceConfig dc;
        dc.device_id   = dev["device_id"].get<std::string>();
        dc.device_type = dev["device_type"].get<std::string>();
        dc.patient_id  = dev["patient_id"].get<std::string>();
        dc.interval_ms = dev["interval_ms"].get<int>();

        for (auto& [key, val] : dev["params"].items())
        {
            ParamRange pr;
            pr.min_val = val["min"].get<double>();
            pr.max_val = val["max"].get<double>();
            pr.initial = val["initial"].get<double>();
            dc.params[key] = pr;
        }
        cfg.devices_.push_back(std::move(dc));
    }

    // 异常配置（可选）
    if (j.contains("anomalies"))
    {
        for (auto& anom : j["anomalies"])
        {
            AnomalyConfig ac;
            ac.device_id = anom["device_id"].get<std::string>();
            ac.type      = anom["type"].get<std::string>();
            ac.trigger_after_sec = anom.value("trigger_after_sec", 60);

            if (anom.contains("params"))
            {
                for (auto& [k, v] : anom["params"].items())
                {
                    ac.params[k] = v.get<double>();
                }
            }
            cfg.anomalies_.push_back(std::move(ac));
        }
    }

    return cfg;
}

}  // namespace device_sim
