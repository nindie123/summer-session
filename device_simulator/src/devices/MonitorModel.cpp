#include "devices/MonitorModel.h"

namespace device_sim {

MonitorModel::MonitorModel(DeviceConfig config)
    : DeviceModel(std::move(config))
{
}

std::vector<Observation> MonitorModel::GenerateData()
{
    std::vector<Observation> results;

    // 心率 HR — LOINC 8867-4
    if (auto it = config_.params.find("heart_rate"); it != config_.params.end())
    {
        double val = GenerateVital(it->second, current_values_["heart_rate"]);
        current_values_["heart_rate"] = val;
        results.push_back({"8867-4", "Heart Rate", val, "/min"});
    }

    // 收缩压 SBP — LOINC 8480-6
    if (auto it = config_.params.find("sys_bp"); it != config_.params.end())
    {
        double val = GenerateVital(it->second, current_values_["sys_bp"]);
        current_values_["sys_bp"] = val;
        results.push_back({"8480-6", "Systolic Blood Pressure", val, "mmHg"});
    }

    // 舒张压 DBP — LOINC 8462-4
    if (auto it = config_.params.find("dia_bp"); it != config_.params.end())
    {
        double val = GenerateVital(it->second, current_values_["dia_bp"]);
        current_values_["dia_bp"] = val;
        results.push_back({"8462-4", "Diastolic Blood Pressure", val, "mmHg"});
    }

    // 血氧饱和度 SpO2 — LOINC 2708-6
    if (auto it = config_.params.find("spo2"); it != config_.params.end())
    {
        double val = GenerateVital(it->second, current_values_["spo2"]);
        current_values_["spo2"] = val;
        results.push_back({"2708-6", "Oxygen Saturation", val, "%"});
    }

    // 呼吸频率 RR — LOINC 9279-1
    if (auto it = config_.params.find("resp_rate"); it != config_.params.end())
    {
        double val = GenerateVital(it->second, current_values_["resp_rate"]);
        current_values_["resp_rate"] = val;
        results.push_back({"9279-1", "Respiratory Rate", val, "/min"});
    }

    return results;
}

}  // namespace device_sim
