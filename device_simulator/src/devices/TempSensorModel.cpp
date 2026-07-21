#include "devices/TempSensorModel.h"

namespace device_sim {

TempSensorModel::TempSensorModel(DeviceConfig config)
    : DeviceModel(std::move(config))
{
}

std::vector<Observation> TempSensorModel::GenerateData()
{
    std::vector<Observation> results;

    // 体温 Temp — LOINC 8310-5
    if (auto it = config_.params.find("temperature"); it != config_.params.end())
    {
        double val = GenerateVital(it->second, current_values_["temperature"]);
        current_values_["temperature"] = val;
        results.push_back({"8310-5", "Body Temperature", val, "°C"});
    }

    return results;
}

}  // namespace device_sim
