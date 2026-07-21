#ifndef SIMULATOR_DEVICE_CONNECTION_H_
#define SIMULATOR_DEVICE_CONNECTION_H_

#include <string>
#include <memory>
#include <functional>
#include <atomic>

#include <asio.hpp>

#include "core/DeviceModel.h"

namespace device_sim {

/// TCP 连接状态
enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    AUTHENTICATING,
    CONNECTED,
    RECONNECTING,
    CLOSED
};

/// 单个设备到采集器的 TCP 连接
class DeviceConnection : public std::enable_shared_from_this<DeviceConnection> {
public:
    using ErrorCallback = std::function<void(const std::string& device_id, const std::string& error)>;

    DeviceConnection(
        asio::io_context& io_context,
        std::unique_ptr<DeviceModel> model,
        std::string host,
        uint16_t port,
        std::string secret
    );
    ~DeviceConnection();

    DeviceConnection(const DeviceConnection&) = delete;
    DeviceConnection& operator=(const DeviceConnection&) = delete;

    /// 启动连接（异步）
    void Start();

    /// 停止连接并关闭
    void Stop();

    /// 发送一条数据（异步）
    void SendData(const std::vector<Observation>& observations);

    /// 设置错误回调
    void SetErrorCallback(ErrorCallback cb) { error_cb_ = std::move(cb); }

    /// 获取当前状态
    ConnectionState state() const { return state_; }

    /// 获取设备 ID
    const std::string& device_id() const { return model_->device_id(); }

    /// 获取设备模型指针（用于异常注入）
    DeviceModel* model() { return model_.get(); }

private:
    void DoConnect();
    void SendAuth();
    void HandleAuthResponse(const asio::error_code& ec, size_t length);
    void StartSendLoop();
    void DoSend(const std::string& data);
    void HandleWrite(const asio::error_code& ec, size_t length);
    void HandleRead(const asio::error_code& ec, size_t length);
    void ScheduleReconnect();
    void Close();

    asio::io_context& io_context_;
    asio::ip::tcp::socket socket_;

    std::unique_ptr<DeviceModel> model_;
    std::string host_;
    uint16_t port_;
    std::string secret_;

    std::atomic<ConnectionState> state_{ConnectionState::DISCONNECTED};
    ErrorCallback error_cb_;

    // 读写缓冲区
    asio::streambuf read_buf_;
    std::vector<uint8_t> write_buf_;

    // 重连
    int reconnect_attempts_{0};
    int max_reconnect_attempts_{5};
    int reconnect_interval_ms_{3000};

    // 定时器（用于控制发送间隔 + 重连）
    asio::steady_timer timer_;
    bool sending_{false};
    int64_t sequence_{0};
};

}  // namespace device_sim

#endif  // SIMULATOR_DEVICE_CONNECTION_H_
