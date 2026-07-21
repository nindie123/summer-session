"""TCP 接入服务器 — 使用 asyncio 处理设备长连接。"""

import asyncio
from collections.abc import Callable
from typing import Any

from src.server.conn_manager import ConnectionManager
from src.pipeline.frame_decoder import FrameDecoder
from src.pipeline.pipeline import ProcessingPipeline
from src.pipeline.parser import parse_auth_message
from src.state.patient_binder import PatientBinder
from src.state.device_tracker import DeviceTracker
from src.kafka.router import Router
from src.observability.logger import get_logger

logger = get_logger(__name__)

# 模拟器密钥（开发环境）
SIMULATOR_SECRET = "sim_secret_2024"


class TcpServer:
    """TCP 接入服务器。

    负责:
      - 监听端口，接收设备连接
      - 每个连接一个协程处理
      - 认证 → 管道处理 → Kafka 投递
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9001,
        max_connections: int = 500,
        kafka_bootstrap_servers: str = "localhost:9092",
    ) -> None:
        self.host = host
        self.port = port
        self.max_connections = max_connections

        self.conn_manager = ConnectionManager()
        self.patient_binder = PatientBinder()
        self.device_tracker = DeviceTracker()
        self.pipeline = ProcessingPipeline()
        self.router = Router(bootstrap_servers=kafka_bootstrap_servers)

        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        """启动 TCP 服务器。"""
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self.host,
            port=self.port,
            limit=1024 * 1024,  # 读缓冲区 1MB
        )

        addr = self._server.sockets[0].getsockname()
        logger.info("tcp_server_started", extra={
            "host": addr[0], "port": addr[1],
            "max_connections": self.max_connections,
        })
        print(f"[COLLECTOR] TCP Server listening on {addr[0]}:{addr[1]}")

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """停止服务器。"""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await self.router.close()
        logger.info("tcp_server_stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """处理单个设备连接。"""
        # 连接数检查
        if self.conn_manager.active_count >= self.max_connections:
            writer.close()
            return

        peer = writer.get_extra_info("peername")
        if peer:
            peer_addr = f"{peer[0]}:{peer[1]}"
        else:
            peer_addr = "unknown"

        # 1. 帧解码器（每个连接独立）
        decoder = FrameDecoder()

        # 2. 等待 auth 消息
        device_id = ""
        patient_id = ""

        try:
            # 读取 auth 消息
            auth_data = await asyncio.wait_for(
                self._read_frame(reader, decoder),
                timeout=30.0,
            )

            auth_info = parse_auth_message(auth_data)
            device_id = auth_info.get("device_id", "")
            patient_id = auth_info.get("patient_id", "")

            # 验证密钥
            if auth_info.get("secret") != SIMULATOR_SECRET:
                logger.warning("auth_failed", extra={
                    "peer": peer_addr, "device_id": device_id,
                    "reason": "invalid_secret",
                })
                await self._send_auth_nack(writer, "invalid_secret")
                writer.close()
                return

            # 注册连接
            conn_id = self.conn_manager.register(
                device_id=device_id,
                patient_id=patient_id,
                peer=peer_addr,
            )

            # 注册患者-设备绑定
            await self.patient_binder.bind(patient_id, device_id)

            # 记录设备在线
            await self.device_tracker.mark_online(device_id, patient_id)

            # 发送 auth_ack
            await self._send_auth_ack(writer, conn_id)

            logger.info("device_authenticated", extra={
                "device_id": device_id, "patient_id": patient_id,
                "conn_id": conn_id, "peer": peer_addr,
            })

            # 3. 数据循环
            await self._data_loop(reader, writer, decoder, device_id, patient_id, conn_id)

        except asyncio.TimeoutError:
            logger.warning("auth_timeout", extra={"peer": peer_addr})
        except ConnectionResetError:
            logger.info("connection_reset", extra={
                "device_id": device_id, "patient_id": patient_id,
            })
        except Exception as e:
            logger.error("connection_error", extra={
                "device_id": device_id, "patient_id": patient_id,
                "error": str(e),
            })
        finally:
            # 清理
            self.conn_manager.unregister(device_id)
            await self.device_tracker.mark_offline(device_id)
            try:
                writer.close()
            except Exception:
                pass

    async def _read_frame(
        self,
        reader: asyncio.StreamReader,
        decoder: FrameDecoder,
    ) -> bytes:
        """从连接读取一个完整的帧。"""
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                raise ConnectionResetError("Connection closed by peer")

            frames = decoder.feed(chunk)
            if frames:
                return frames[0]

    async def _data_loop(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        decoder: FrameDecoder,
        device_id: str,
        patient_id: str,
        conn_id: str,
    ) -> None:
        """数据处理循环。"""
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break

            frames = decoder.feed(chunk)

            for raw_frame in frames:
                # 更新连接活跃时间
                self.conn_manager.update_activity(device_id)

                # 处理管道
                import datetime
                received_at = datetime.datetime.now(
                    datetime.timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S.") + \
                    f"{datetime.datetime.now(datetime.timezone.utc).microsecond // 1000:03d}Z"

                ctx = await self.pipeline.process(raw_frame, "", received_at)

                if ctx.aborted:
                    logger.warning("frame_aborted", extra={
                        "device_id": device_id,
                        "reason": ctx.abort_reason,
                    })
                    await self.router.route_failed(raw_frame, ctx.abort_reason)
                    continue

                # Kafka 投递
                await self.router.route(ctx.record)

                # 指标更新
                logger.info("frame_processed", extra={
                    "device_id": device_id,
                    "patient_id": patient_id,
                    "observations": len(ctx.record.observations) if ctx.record else 0,
                    "elapsed_ms": round(ctx.elapsed_ms, 2),
                    "validation": ctx.record.processing.validation_status if ctx.record else "unknown",
                })

    async def _send_auth_ack(
        self,
        writer: asyncio.StreamWriter,
        conn_id: str,
    ) -> None:
        """发送认证成功响应。"""
        import json
        ack = json.dumps({
            "type": "auth_ack",
            "status": "ok",
            "connId": conn_id,
        }).encode()
        # 包成 Length+JSON 帧
        payload_len = len(ack)
        header = payload_len.to_bytes(4, "big")
        writer.write(header + ack)
        await writer.drain()

    async def _send_auth_nack(
        self,
        writer: asyncio.StreamWriter,
        reason: str,
    ) -> None:
        """发送认证失败响应。"""
        import json
        nack = json.dumps({
            "type": "auth_ack",
            "status": "error",
            "reason": reason,
        }).encode()
        payload_len = len(nack)
        header = payload_len.to_bytes(4, "big")
        writer.write(header + nack)
        await writer.drain()
