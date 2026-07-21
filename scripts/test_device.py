#!/usr/bin/env python3
"""
设备模拟器 — 模拟 50 个患者，包含多种临床场景。
协议: TCP + Length(4B Big-Endian) + JSON
"""

import asyncio
import json
import os
import random
import struct
import time

random.seed(42)

# ── 临床场景配置 ──────────────────────────────────
# 每个场景定义了患者的体征参数范围和行为
PATIENT_PROFILES = {
    "healthy": {        # 健康患者（占大多数）
        "hr":  (60, 100, 72),
        "sbp": (100, 140, 120),
        "dbp": (60, 90, 80),
        "spo2":(95, 100, 98),
        "rr":  (12, 20, 16),
        "temp":(36.0, 37.5, 36.8),
        "weight": 30,  # 30个
    },
    "hypertensive": {   # 高血压
        "hr":  (60, 100, 75),
        "sbp": (150, 200, 165),
        "dbp": (90, 120, 100),
        "spo2":(93, 98, 96),
        "rr":  (14, 22, 18),
        "temp":(36.0, 37.5, 36.6),
        "weight": 4,
    },
    "tachycardia": {    # 心动过速
        "hr":  (110, 140, 120),
        "sbp": (100, 130, 115),
        "dbp": (60, 85, 75),
        "spo2":(94, 99, 97),
        "rr":  (18, 26, 22),
        "temp":(36.5, 38.0, 37.2),
        "weight": 3,
    },
    "bradycardia": {    # 心动过缓
        "hr":  (40, 55, 48),
        "sbp": (90, 120, 105),
        "dbp": (55, 75, 65),
        "spo2":(93, 98, 96),
        "rr":  (10, 16, 13),
        "temp":(35.8, 37.0, 36.4),
        "weight": 2,
    },
    "hypoxic": {        # 低血氧
        "hr":  (85, 120, 100),
        "sbp": (100, 130, 115),
        "dbp": (60, 85, 72),
        "spo2":(85, 92, 90),
        "rr":  (20, 30, 24),
        "temp":(36.5, 38.0, 37.0),
        "weight": 3,
    },
    "febrile": {        # 发热
        "hr":  (90, 120, 105),
        "sbp": (100, 130, 115),
        "dbp": (60, 85, 72),
        "spo2":(94, 99, 97),
        "rr":  (18, 26, 22),
        "temp":(38.5, 40.0, 39.2),
        "weight": 3,
    },
    "deteriorating": {  # 病情恶化（随时间恶化）
        "hr":  (75, 140, 80),
        "sbp": (100, 140, 120),
        "dbp": (60, 90, 80),
        "spo2":(98, 85, 97),
        "rr":  (14, 30, 16),
        "temp":(36.5, 39.5, 37.0),
        "weight": 3,
    },
    "severe": {         # 重症（多项异常）
        "hr":  (120, 160, 135),
        "sbp": (80, 100, 90),
        "dbp": (50, 65, 58),
        "spo2":(80, 90, 86),
        "rr":  (28, 40, 32),
        "temp":(38.0, 40.0, 38.8),
        "weight": 2,
    },
}


def build_profiles() -> list[dict]:
    """生成 50 个患者的配置列表。"""
    profiles = []
    pid = 1

    for scene_name, scene in PATIENT_PROFILES.items():
        for _ in range(scene["weight"]):
            patient_id = f"P{pid:04d}"
            profiles.append({
                "patient_id": patient_id,
                "bed_id": f"ICU-{100 + pid}",
                "scene": scene_name,
                "params": scene,
            })
            pid += 1

    # 补齐到50个（用healthy补）
    while len(profiles) < 50:
        pid += 1
        patient_id = f"P{pid:04d}"
        h = PATIENT_PROFILES["healthy"]
        profiles.append({
            "patient_id": patient_id,
            "bed_id": f"ICU-{100 + pid}",
            "scene": "healthy",
            "params": h,
        })

    return profiles[:50]


def generate_vital(profile: dict, param: str, prev: float, elapsed: float) -> float:
    """生成一个体征值，考虑场景特征和时间变化。"""
    lo, hi, init = profile["params"][param]
    scene = profile["scene"]
    base = prev if prev else init
    noise = random.gauss(0, (hi - lo) * 0.02)

    value = base + noise

    # deteriorate 场景：随时间恶化
    if scene == "deteriorating":
        decay_rate = elapsed / 30.0  # 30秒内逐渐恶化
        if param == "hr":
            value += decay_rate * 50.0  # HR 升高
        elif param == "spo2":
            value -= decay_rate * 12.0  # SpO2 下降
        elif param == "temp":
            value += decay_rate * 2.5   # 体温升高
        elif param == "rr":
            value += decay_rate * 15.0  # 呼吸加快
        elif param in ("sbp", "dbp"):
            value -= decay_rate * 20.0  # BP 下降

    # 钳制
    value = max(lo, min(hi, value))
    return round(value, 1)


# ── 设备类 ─────────────────────────────────────────
class SimulatedDevice:
    """模拟单个设备。"""

    def __init__(self, device_id: str, device_type: str, patient_id: str,
                 host: str, port: int, interval_ms: int, profile: dict):
        self.device_id = device_id
        self.device_type = device_type
        self.patient_id = patient_id
        self.host = host
        self.port = port
        self.interval_ms = interval_ms
        self.profile = profile
        self.seq = 0
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.values: dict[str, float] = {}
        self.start_time = time.time()

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        # 等采集器完全就绪再发 auth
        await asyncio.sleep(0.05)
        auth = json.dumps({
            "type": "auth",
            "deviceId": self.device_id,
            "deviceType": self.device_type,
            "patientId": self.patient_id,
            "secret": "sim_secret_2024",
        })
        self._send_frame(auth)
        await self.writer.drain()
        data = await asyncio.wait_for(self._read_frame(), timeout=10)

    def generate(self) -> list[dict]:
        elapsed = time.time() - self.start_time
        obs = []
        if self.device_type == "Monitor":
            for code, name, param in [
                ("8867-4", "Heart Rate", "hr"),
                ("8480-6", "Systolic BP", "sbp"),
                ("8462-4", "Diastolic BP", "dbp"),
                ("2708-6", "Oxygen Saturation", "spo2"),
                ("9279-1", "Respiratory Rate", "rr"),
            ]:
                prev = self.values.get(param)
                val = generate_vital(self.profile, param, prev, elapsed)
                self.values[param] = val
                obs.append({"code": code, "name": name, "value": val,
                            "unit": "/min" if param in ("hr", "rr") else "mmHg" if "bp" in param else "%"})
        else:
            prev = self.values.get("temp")
            val = generate_vital(self.profile, "temp", prev, elapsed)
            self.values["temp"] = val
            obs.append({"code": "8310-5", "name": "Body Temperature", "value": val, "unit": "°C"})
        return obs

    async def send_vitals(self, observations: list[dict]):
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
        payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        frame = struct.pack("!I", len(payload)) + payload
        self.writer.write(frame)

    async def _read_frame(self) -> str:
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


async def run_device(device: SimulatedDevice, duration_sec: int = 35, log: bool = False):
    """运行一个设备循环（含重试）。"""
    # 重试连接
    connected = False
    for attempt in range(5):
        try:
            await device.connect()
            connected = True
            break
        except (ConnectionResetError, ConnectionRefusedError, OSError, asyncio.IncompleteReadError) as e:
            if log:
                print(f"  ⏳ {device.device_id} 连接失败(attempt {attempt+1}): {e}")
            await asyncio.sleep(1)
            continue

    if not connected:
        if log:
            print(f"  ❌ {device.device_id} 连接失败（放弃）")
        return

    if log:
        print(f"  [+] {device.device_id} ({device.patient_id}, {device.profile['scene']})")

    try:
        end_time = time.time() + duration_sec
        while time.time() < end_time:
            obs = device.generate()
            await device.send_vitals(obs)
            await asyncio.sleep(device.interval_ms / 1000)
    except (ConnectionResetError, OSError) as e:
        if log:
            print(f"  [-] {device.device_id} 断开: {e}")
    finally:
        await device.close()


def print_scene_summary(profiles: list[dict]):
    """打印患者场景分布。"""
    scenes: dict[str, int] = {}
    for p in profiles:
        scenes[p["scene"]] = scenes.get(p["scene"], 0) + 1
    print(f"  📊 患者分布: {scenes}")
    print(f"     总计: {len(profiles)} 人")
    print()
    for p in profiles[:8]:
        print(f"     {p['patient_id']} ({p['bed_id']}) → {p['scene']}")
    if len(profiles) > 8:
        print(f"     ... 共 {len(profiles)} 个患者")


async def main():
    host = os.environ.get("COLLECTOR_HOST", "127.0.0.1")
    port = int(os.environ.get("COLLECTOR_PORT", "9001"))
    duration = int(os.environ.get("SIM_DURATION_SEC", "35"))

    print("=" * 70)
    print("  🏥 医院设备模拟器 - 50 患者多场景数据")
    print(f"  目标: {host}:{port}")
    print("=" * 70)
    print()

    # 生成50个患者配置
    profiles = build_profiles()
    print_scene_summary(profiles)

    # 为每个患者创建 Monitor + TempSensor
    devices = []
    for p in profiles:
        pid = p["patient_id"][1:]  # "P0001" → "0001"
        devices.append(SimulatedDevice(
            f"monitor_{pid}", "Monitor", p["patient_id"],
            host, port, 1000, p))
        devices.append(SimulatedDevice(
            f"temp_{pid}", "TempSensor", p["patient_id"],
            host, port, 30000, p))

    print()
    print(f"  🚀 {len(profiles)} 个患者 × 2 设备 = {len(devices)} 连接")
    print(f"  ⏱  持续 {duration} 秒")
    print(f"  🔗 分批连接（每批 5 个，间隔 2 秒）...")
    print()

    # 分批连接：每批5个，间隔2秒，避免采集器过载
    connected_devices = []
    batch_size = 5
    total_batches = (len(devices) + batch_size - 1) // batch_size

    for batch_start in range(0, len(devices), batch_size):
        batch = devices[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1

        async def connect_one_device(d, idx):
            for attempt in range(3):
                try:
                    await d.connect()
                    return d
                except Exception as e:
                    if idx < 5:  # 只打印前5个的错误
                        print(f"    [dbg] {d.device_id} attempt {attempt+1}: {type(e).__name__}: {str(e)[:60]}")
                    await asyncio.sleep(0.5)
            return None

        results = await asyncio.gather(*[connect_one_device(d, batch_start+i) for i, d in enumerate(batch)])
        success = sum(1 for r in results if r is not None)
        connected_devices.extend([r for r in results if r is not None])
        print(f"  📡 批次 {batch_num}/{total_batches}: {success}/{len(batch)} 连接成功")

        if batch_start + batch_size < len(devices):
            await asyncio.sleep(2)  # 给采集器喘息时间

    print(f"\n  ✅ {len(connected_devices)}/{len(devices)} 设备已连接, 开始发送数据...\n")

    # 第二步：所有已连接的设备同时发送
    async def send_loop(device):
        try:
            end_time = time.time() + duration
            while time.time() < end_time:
                obs = device.generate()
                await device.send_vitals(obs)
                await asyncio.sleep(device.interval_ms / 1000)
        except Exception:
            pass
        finally:
            await device.close()

    start = time.time()
    await asyncio.gather(*[send_loop(d) for d in connected_devices])
    elapsed = time.time() - start

    print()
    print("=" * 70)
    print(f"  ✅ 完成! 耗时 {elapsed:.0f} 秒")
    print(f"  📊 共模拟 {len(profiles)} 个患者, {len(devices)} 个设备")
    print("=" * 70)
    print()
    print("  查看结果:")
    print("    http://localhost:8000/docs")
    print("    http://localhost:8000/api/v1/wards/ICU-EAST/overview")
    print()


if __name__ == "__main__":
    asyncio.run(main())
