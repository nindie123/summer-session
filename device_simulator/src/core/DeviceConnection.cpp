#include "core/DeviceConnection.h"
#include "protocol/FrameProtocol.h"

#include <iostream>
#include <system_error>

namespace device_sim {

DeviceConnection::DeviceConnection(
    asio::io_context& io_context,
    std::unique_ptr<DeviceModel> model,
    std::string host,
    uint16_t port,
    std::string secret)
    : io_context_(io_context)
    , socket_(io_context)
    , model_(std::move(model))
    , host_(std::move(host))
    , port_(port)
    , secret_(std::move(secret))
    , timer_(io_context)
{
}

DeviceConnection::~DeviceConnection()
{
    Close();
}

void DeviceConnection::Start()
{
    std::cout << "[DEVICE] " << model_->device_id()
              << " starting, target " << host_ << ":" << port_
              << " type=" << model_->DeviceType()
              << " patient=" << model_->config().patient_id
              << std::endl;
    state_ = ConnectionState::CONNECTING;
    DoConnect();
}

void DeviceConnection::Stop()
{
    state_ = ConnectionState::CLOSED;
    asio::error_code ec;
    socket_.close(ec);
    timer_.cancel();
}

void DeviceConnection::DoConnect()
{
    auto self = shared_from_this();
    socket_.async_connect(
        asio::ip::tcp::endpoint(
            asio::ip::address::from_string(host_), port_),
        [this, self](const asio::error_code& ec)
        {
            if (state_ == ConnectionState::CLOSED) return;

            if (ec)
            {
                std::cerr << "[DEVICE] " << model_->device_id()
                          << " connect failed: " << ec.message() << std::endl;
                ScheduleReconnect();
                return;
            }

            std::cout << "[DEVICE] " << model_->device_id()
                      << " connected to " << host_ << ":" << port_ << std::endl;
            state_ = ConnectionState::AUTHENTICATING;
            SendAuth();
        });
}

void DeviceConnection::SendAuth()
{
    json auth_msg = {
        {"type", "auth"},
        {"deviceId", model_->device_id()},
        {"deviceType", model_->DeviceType()},
        {"patientId", model_->config().patient_id},
        {"secret", secret_}
    };

    auto frame = FrameProtocol::Encode(auth_msg);
    DoSend(std::string(frame.begin(), frame.end()));

    // 等待 auth_ack
    auto self = shared_from_this();
    asio::async_read_until(socket_, read_buf_, '\n',
        [this, self](const asio::error_code& ec, size_t length)
        {
            HandleAuthResponse(ec, length);
        });
}

void DeviceConnection::HandleAuthResponse(const asio::error_code& ec, size_t length)
{
    if (ec || state_ == ConnectionState::CLOSED)
    {
        std::cerr << "[DEVICE] " << model_->device_id()
                  << " auth failed: " << (ec ? ec.message() : "closed") << std::endl;
        ScheduleReconnect();
        return;
    }

    // 读取响应（简化为读取一行）
    std::string response((std::istreambuf_iterator<char>(&read_buf_)),
                          std::istreambuf_iterator<char>());
    read_buf_.consume(read_buf_.size());

    std::cout << "[DEVICE] " << model_->device_id()
              << " authenticated, response: " << response.substr(0, 100) << std::endl;

    state_ = ConnectionState::CONNECTED;
    reconnect_attempts_ = 0;

    // 开始数据发送循环
    StartSendLoop();
}

void DeviceConnection::StartSendLoop()
{
    if (state_ != ConnectionState::CONNECTED || sending_) return;

    sending_ = true;
    auto self = shared_from_this();

    timer_.expires_after(std::chrono::milliseconds(model_->interval_ms()));
    timer_.async_wait([this, self](const asio::error_code& ec)
    {
        if (ec || state_ != ConnectionState::CONNECTED)
        {
            sending_ = false;
            return;
        }

        // 生成并发送数据
        auto observations = model_->GenerateData();
        SendData(observations);

        sending_ = false;
        // 继续下一轮
        if (state_ == ConnectionState::CONNECTED)
        {
            StartSendLoop();
        }
    });
}

void DeviceConnection::SendData(const std::vector<Observation>& observations)
{
    json vitals_msg;
    vitals_msg["type"] = "vitals";
    vitals_msg["deviceId"] = model_->device_id();
    vitals_msg["deviceType"] = model_->DeviceType();
    vitals_msg["patientId"] = model_->config().patient_id;

    // 当前时间（ISO 8601 UTC）
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;

    char timestamp[32];
    std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", std::gmtime(&tt));
    vitals_msg["timestamp"] = std::string(timestamp) + "." +
                               std::to_string(ms.count()) + "Z";

    vitals_msg["sequence"] = sequence_++;

    json obs_array = json::array();
    for (const auto& obs : observations)
    {
        obs_array.push_back({
            {"code", obs.code},
            {"name", obs.name},
            {"value", obs.value},
            {"unit", obs.unit}
        });
    }
    vitals_msg["observations"] = obs_array;

    auto frame = FrameProtocol::Encode(vitals_msg);
    DoSend(std::string(frame.begin(), frame.end()));
}

void DeviceConnection::DoSend(const std::string& data)
{
    auto self = shared_from_this();
    write_buf_.assign(data.begin(), data.end());

    asio::async_write(socket_,
        asio::buffer(write_buf_),
        [this, self](const asio::error_code& ec, size_t)
        {
            if (ec)
            {
                std::cerr << "[DEVICE] " << model_->device_id()
                          << " send error: " << ec.message() << std::endl;
                if (error_cb_)
                    error_cb_(model_->device_id(), ec.message());
            }
        });
}

void DeviceConnection::ScheduleReconnect()
{
    if (state_ == ConnectionState::CLOSED) return;

    state_ = ConnectionState::RECONNECTING;
    reconnect_attempts_++;

    if (reconnect_attempts_ > max_reconnect_attempts_)
    {
        std::cerr << "[DEVICE] " << model_->device_id()
                  << " max reconnect attempts reached, giving up." << std::endl;
        state_ = ConnectionState::CLOSED;
        return;
    }

    std::cout << "[DEVICE] " << model_->device_id()
              << " reconnecting in " << reconnect_interval_ms_
              << "ms (attempt " << reconnect_attempts_
              << "/" << max_reconnect_attempts_ << ")" << std::endl;

    timer_.expires_after(std::chrono::milliseconds(reconnect_interval_ms_));
    auto self = shared_from_this();
    timer_.async_wait([this, self](const asio::error_code& ec)
    {
        if (ec || state_ == ConnectionState::CLOSED) return;
        state_ = ConnectionState::CONNECTING;
        asio::error_code ignore_ec;
        socket_.close(ignore_ec);
        socket_ = asio::ip::tcp::socket(io_context_);
        DoConnect();
    });
}

void DeviceConnection::Close()
{
    state_ = ConnectionState::CLOSED;
    asio::error_code ec;
    socket_.close(ec);
    timer_.cancel();
}

}  // namespace device_sim
