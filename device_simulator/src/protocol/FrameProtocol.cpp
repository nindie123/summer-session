#include "protocol/FrameProtocol.h"
#include <cstring>
#include <stdexcept>

namespace device_sim {

std::vector<uint8_t> FrameProtocol::Encode(const json& message)
{
    std::string payload = message.dump();
    size_t payload_len = payload.size();

    if (payload_len > kMaxMessageSize)
    {
        throw std::runtime_error(
            "Message too large: " + std::to_string(payload_len) + " bytes");
    }

    std::vector<uint8_t> frame(kHeaderSize + payload_len);

    // 写 4 字节 Big-Endian 长度头
    frame[0] = static_cast<uint8_t>((payload_len >> 24) & 0xFF);
    frame[1] = static_cast<uint8_t>((payload_len >> 16) & 0xFF);
    frame[2] = static_cast<uint8_t>((payload_len >> 8) & 0xFF);
    frame[3] = static_cast<uint8_t>(payload_len & 0xFF);

    // 写 JSON Payload
    std::memcpy(frame.data() + kHeaderSize, payload.data(), payload_len);

    return frame;
}

std::optional<std::string> FrameProtocol::Decode(
    const uint8_t* data, size_t length, size_t& frame_size)
{
    frame_size = 0;

    if (length < kHeaderSize)
    {
        // 数据太少，还没收到完整的长度头
        return std::nullopt;
    }

    // 读取 4 字节 Big-Endian 长度
    uint32_t payload_len =
        (static_cast<uint32_t>(data[0]) << 24) |
        (static_cast<uint32_t>(data[1]) << 16) |
        (static_cast<uint32_t>(data[2]) << 8)  |
        static_cast<uint32_t>(data[3]);

    if (payload_len > kMaxMessageSize)
    {
        throw std::runtime_error(
            "Invalid frame: payload length " + std::to_string(payload_len)
            + " exceeds max " + std::to_string(kMaxMessageSize));
    }

    size_t total_len = kHeaderSize + payload_len;
    if (length < total_len)
    {
        // 还没收完整整帧，等待更多数据
        return std::nullopt;
    }

    std::string payload(
        reinterpret_cast<const char*>(data + kHeaderSize), payload_len);

    frame_size = total_len;
    return payload;
}

}  // namespace device_sim
