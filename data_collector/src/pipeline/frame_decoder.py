"""Length + JSON 帧解码器 — 处理 TCP 粘包/拆包。"""

from collections.abc import Callable
import struct


class FrameDecoder:
    """从 TCP 字节流中提取完整的 Length+JSON 帧。

    帧格式:
      [4 bytes: Payload Length (Big-Endian)]
      [N bytes: JSON Payload (UTF-8)]
    """

    HEADER_SIZE = 4
    MAX_FRAME_SIZE = 1 * 1024 * 1024  # 1MB

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        """向解码器喂入原始字节数据，返回已提取的完整消息帧列表。

        Args:
            data: 从 TCP 连接读取的原始字节。

        Returns:
            完整消息帧的 JSON payload 列表（可能为空）。
        """
        self._buffer.extend(data)
        frames: list[bytes] = []

        while True:
            if len(self._buffer) < self.HEADER_SIZE:
                break

            # 读取 4 字节 Big-Endian 长度头
            payload_len = struct.unpack("!I", self._buffer[:4])[0]

            if payload_len > self.MAX_FRAME_SIZE:
                # 非法帧，清空缓冲区防止内存溢出
                self._buffer.clear()
                msg = f"Frame too large: {payload_len} > {self.MAX_FRAME_SIZE}"
                raise ValueError(msg)

            total_len = self.HEADER_SIZE + payload_len

            if len(self._buffer) < total_len:
                # 还没收完整帧，等待更多数据
                break

            # 提取完整帧
            frame = bytes(self._buffer[self.HEADER_SIZE : total_len])
            frames.append(frame)

            # 移除已处理的数据
            del self._buffer[:total_len]

        return frames

    def reset(self) -> None:
        """重置缓冲区（连接断开时调用）。"""
        self._buffer.clear()

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)
