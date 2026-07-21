#ifndef SIMULATOR_MONITOR_MODEL_H_
#define SIMULATOR_MONITOR_MODEL_H_

#include "core/DeviceModel.h"

namespace device_sim {

/// 监护仪设备 — 产生 HR, BP, SpO2, RR
class MonitorModel final : public DeviceModel {
public:
    explicit MonitorModel(DeviceConfig config);

    std::vector<Observation> GenerateData() override;
    std::string DeviceType() const override { return "Monitor"; }
};

}  // namespace device_sim

#endif  // SIMULATOR_MONITOR_MODEL_H_
