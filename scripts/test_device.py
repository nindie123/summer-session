#!/usr/bin/env python3
"""
Python 测试设备 — 模拟 C++ 模拟器协议，用于验证采集层链路。
协议: TCP + Length(4B Big-Endian) + JSON
"""

import asyncio
import json
import os
import random
import struct
import time


class SimulatedDevice:
    """模拟单个设备。"""

    def __init__(
        self,
        device_id: str,
        device_type: str,
        patient_id: str,
        host: str = "127.0.0.1",
        port: int = 9001,
        interval_ms: int = 1000,
    ):
        self.device_id = device_id
        self.device_type = device_type
        self.patient_id = patient_id
        self.host = host
        self.port = port
        self.interval_ms = interval_ms
        self.seq = 0
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        # 各参数当前值（用作生成基准）
        self.values: dict[str, float] = {}

    async def connect(self):
        """TCP 连接 + 发送 auth。"""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

        # 发送 auth
        auth = {
            "type": "auth",
            "deviceId": self.device_id,
            "deviceType": self.device_type,
            "patientId": self.patient_id,
            "secret": "sim_secret_2024",
        }
        self._send_frame(auth)
        print(f"  [DEVICE] {self.device_id} auth sent, waiting for ack...")

        # 读取 auth_ack
        data = await asyncio.wait_for(self._read_frame(), timeout=10)
        print(f"  [DEVICE] {self.device_id} authenticated: {data[:80]}...")
        return True

    async def send_vitals(self, observations: list[dict]):
        """发送一条体征数据。"""
        self.seq += 1
        now = time.time()
        ts = time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime(now)) + f"{int(now * 1000) % 1000:03d}Z"

        msg = {
            "type": "vitals",
            "deviceId": self.device_id,
            "deviceType": self.device_type,
            "patientId": self.patient_id,
            "timestamp": ts,
            "sequence": self.seq,
            "observations": observations,
        }
        self._send_frame(msg)

    def _send_frame(self, msg: dict):
        """Length + JSON 帧编码并发送。"""
        payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        frame = struct.pack("!I", len(payload)) + payload
        self.writer.write(frame)

    async def _read_frame(self) -> str:
        """读取一个 Length+JSON 帧。"""
        header = await self.reader.readexactly(4)
        payload_len = struct.unpack("!I", header)[0]
        payload = await self.reader.readexactly(payload_len)
        return payload.decode("utf-8")

    async def close(self):
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass

    def _clamp(self, val, lo, hi):
        return max(lo, min(hi, round(val, 1)))


class MonitorDevice(SimulatedDevice):
    """模拟监护仪 — HR, SBP, DBP, SpO2, RR"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.values = {"hr": 72, "sbp": 120, "dbp": 80, "spo2": 98, "rr": 16}

    def generate(self) -> list[dict]:
        hrv = self._clamp(self.values["hr"] + random.gauss(0, 2), 60, 100)
        sbpv = self._clamp(self.values["sbp"] + random.gauss(0, 3), 100, 140)
        dbpv = self._clamp(self.values["dbp"] + random.gauss(0, 2), 60, 90)
        spo2v = self._clamp(self.values["spo2"] + random.gauss(0, 0.5), 95, 100)
        rrv = self._clamp(self.values["rr"] + random.gauss(0, 1), 12, 20)
        self.values.update({"hr": hrv, "sbp": sbpv, "dbp": dbpv, "spo2": spo2v, "rr": rrv})

        return [
            {"code": "8867-4", "name": "Heart Rate",             "value": hrv,  "unit": "/min"},
            {"code": "8480-6", "name": "Systolic BP",            "value": sbpv, "unit": "mmHg"},
            {"code": "8462-4", "name": "Diastolic BP",           "value": dbpv, "unit": "mmHg"},
            {"code": "2708-6", "name": "Oxygen Saturation",      "value": spo2v,"unit": "%"},
            {"code": "9279-1", "name": "Respiratory Rate",       "value": rrv,  "unit": "/min"},
        ]


class TempSensorDevice(SimulatedDevice):
    """模拟体温探头 — Temp"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.values = {"temp": 36.8}

    def generate(self) -> list[dict]:
        tv = self._clamp(self.values["temp"] + random.gauss(0, 0.1), 36.0, 37.5)
        self.values["temp"] = tv
        return [
            {"code": "8310-5", "name": "Body Temperature", "value": tv, "unit": "°C"},
        ]


async def run_device(device: SimulatedDevice, duration_sec: int = 30):
    """启动一个设备循环。"""
    try:
        await device.connect()
        end_time = time.time() + duration_sec
        count = 0
        while time.time() < end_time:
            obs = device.generate()
            await device.send_vitals(obs)
            count += 1
            print(f"    {device.device_id} sent {len(obs)} observations (seq={device.seq})")
            await asyncio.sleep(device.interval_ms / 1000)
        print(f"  [DEVICE] {device.device_id} done, sent {count} messages")
    except Exception as e:
        print(f"  [DEVICE] {device.device_id} error: {e}")
    finally:
        await device.close()


async def main():
    host = os.environ.get("COLLECTOR_HOST", "127.0.0.1")
    port = int(os.environ.get("COLLECTOR_PORT", "9001"))

    print("=" * 60)
    print("Device Simulator (Python version)")
    print(f"Target: {host}:{port}")
    print("=" * 60)

    devices = [
        MonitorDevice("monitor_001", "Monitor", "P0001", host=host, port=port, interval_ms=1000),
        MonitorDevice("monitor_002", "Monitor", "P0002", host=host, port=port, interval_ms=1000),
        TempSensorDevice("temp_001", "TempSensor", "P0001", host=host, port=port, interval_ms=30000),
    ]

    print(f"\nStarting {len(devices)} devices...\n")
    await asyncio.gather(*[run_device(d, duration_sec=30) for d in devices])

    print("\n" + "=" * 60)
    print("All devices finished.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
