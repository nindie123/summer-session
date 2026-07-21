#ifndef SIMULATOR_TEMP_SENSOR_MODEL_H_
#define SIMULATOR_TEMP_SENSOR_MODEL_H_

#include "core/DeviceModel.h"

namespace device_sim {

/// 体温探头 — 产生体温 (T1)
class TempSensorModel final : public DeviceModel {
public:
    explicit TempSensorModel(DeviceConfig config);

    std::vector<Observation> GenerateData() override;
    std::string DeviceType() const override { return "TempSensor"; }
};

}  // namespace device_sim

#endif  // SIMULATOR_TEMP_SENSOR_MODEL_H_
