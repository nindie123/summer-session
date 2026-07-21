#ifndef SIMULATOR_FRAME_PROTOCOL_H_
#define SIMULATOR_FRAME_PROTOCOL_H_

#include <vector>
#include <cstdint>
#include <string>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace device_sim {

/// Length + JSON 帧协议编解码
///
/// 帧格式:
///   [4 bytes: Payload Length (Big-Endian)]
///   [N bytes: JSON Payload (UTF-8)]
///
/// 示例:
///   0x00 0x00 0x00 0xB4
///   {"type":"vitals","deviceId":"monitor_001",...}
class FrameProtocol {
public:
    /// 将 JSON 消息编码为 Length + JSON 帧
    static std::vector<uint8_t> Encode(const json& message);

    /// 从字节缓冲区尝试解码一条消息
    /// @return 解码成功返回完整帧的消息体 JSON 字符串, 失败返回空
    static std::optional<std::string> Decode(const uint8_t* data, size_t length, size_t& frame_size);

    /// 单条消息的最大长度 (1MB)
    static constexpr size_t kMaxMessageSize = 1 * 1024 * 1024;

    /// 长度头大小
    static constexpr size_t kHeaderSize = 4;
};

}  // namespace device_sim

#endif  // SIMULATOR_FRAME_PROTOCOL_H_
