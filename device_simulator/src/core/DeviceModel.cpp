#include "core/DeviceModel.h"
#include <cmath>

namespace device_sim {

DeviceModel::DeviceModel(DeviceConfig config)
    : config_(std::move(config))
{
    // 初始化各参数为 initial 值
    for (const auto& [key, pr] : config_.params)
    {
        current_values_[key] = pr.initial;
    }
}

void DeviceModel::OverrideParam(const std::string& param, double value)
{
    overrides_[param] = value;
    current_values_[param] = value;
}

void DeviceModel::ClearOverride(const std::string& param)
{
    overrides_.erase(param);
    // 恢复为范围内随机值
    auto it = config_.params.find(param);
    if (it != config_.params.end())
    {
        current_values_[param] = it->second.initial;
    }
}

double DeviceModel::CurrentValue(const std::string& param) const
{
    auto it = current_values_.find(param);
    if (it != current_values_.end())
        return it->second;
    return 0.0;
}

double DeviceModel::GenerateVital(const ParamRange& range, double prev_value)
{
    // 如果该参数有覆盖值，返回覆盖值
    for (const auto& [key, val] : overrides_)
    {
        for (const auto& [pk, pr] : config_.params)
        {
            if (pk == key && std::abs(pr.min_val - range.min_val) < 0.01)
            {
                // 匹配到覆盖参数，返回覆盖值
                auto ov = overrides_.find(key);
                if (ov != overrides_.end())
                    return ov->second;
            }
        }
    }

    // 正常生理波动：±3%
    std::normal_distribution<double> dist(0.0, (range.max_val - range.min_val) * 0.03);
    double delta = dist(rng_);
    double new_val = prev_value + delta;

    // 钳制在范围内
    new_val = std::max(range.min_val, std::min(range.max_val, new_val));
    return std::round(new_val * 10.0) / 10.0;  // 保留一位小数
}

}  // namespace device_sim
