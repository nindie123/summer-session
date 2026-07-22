#!/usr/bin/env python3
"""
8 患者临床模拟器 — 所有数据动态变化。
每个患者的每个体征参数独立波动，有周期、趋势、突变。
"""

import asyncio, json, os, random, struct, time, math

random.seed(42)

PATIENTS = [
    {"pid":"P001","bed":"ICU-101","scene":"healthy",        "hr":(60,100,72),"sbp":(100,140,120),"dbp":(60,90,80),"spo2":(95,100,98),"rr":(12,20,16),"temp":(36.0,37.5,36.8)},
    {"pid":"P002","bed":"ICU-102","scene":"hypertensive",   "hr":(60,100,75),"sbp":(150,200,165),"dbp":(90,120,100),"spo2":(93,98,96),"rr":(14,22,18),"temp":(36.0,37.5,36.6)},
    {"pid":"P003","bed":"ICU-103","scene":"tachycardia",    "hr":(110,140,120),"sbp":(100,130,115),"dbp":(60,85,75),"spo2":(94,99,97),"rr":(18,26,22),"temp":(36.5,38.0,37.2)},
    {"pid":"P004","bed":"ICU-104","scene":"bradycardia",    "hr":(40,55,48),"sbp":(90,120,105),"dbp":(55,75,65),"spo2":(93,98,96),"rr":(10,16,13),"temp":(35.8,37.0,36.4)},
    {"pid":"P005","bed":"ICU-105","scene":"hypoxic",        "hr":(85,120,100),"sbp":(100,130,115),"dbp":(60,85,72),"spo2":(85,92,90),"rr":(20,30,24),"temp":(36.5,38.0,37.0)},
    {"pid":"P006","bed":"ICU-106","scene":"febrile",        "hr":(90,120,105),"sbp":(100,130,115),"dbp":(60,85,72),"spo2":(94,99,97),"rr":(18,26,22),"temp":(38.5,40.0,39.2)},
    {"pid":"P007","bed":"ICU-107","scene":"deteriorating",  "hr":(75,140,80),"sbp":(100,140,120),"dbp":(60,90,80),"spo2":(98,85,97),"rr":(14,30,16),"temp":(36.5,39.5,37.0)},
    {"pid":"P008","bed":"ICU-108","scene":"severe",         "hr":(120,160,135),"sbp":(80,100,90),"dbp":(50,65,58),"spo2":(80,90,86),"rr":(28,40,32),"temp":(38.0,40.0,38.8)},
]

CODES = {"hr":"8867-4","sbp":"8480-6","dbp":"8462-4","spo2":"2708-6","rr":"9279-1","temp":"8310-5"}
NAMES = {"hr":"Heart Rate","sbp":"Systolic BP","dbp":"Diastolic BP","spo2":"Oxygen Saturation","rr":"Respiratory Rate","temp":"Body Temperature"}
UNITS = {"hr":"/min","rr":"/min","sbp":"mmHg","dbp":"mmHg","spo2":"%","temp":"°C"}

# ── 每个患者的参数波动配置 ─────────────────────────
# 为每个(患者,参数)设置独立的: 周期(秒), 振幅(范围占比), 趋势方向
def build_wave_configs():
    configs = {}
    for p in PATIENTS:
        pid = p["pid"]
        configs[pid] = {}
        params = ["hr","sbp","dbp","spo2","rr","temp"]
        for param in params:
            lo, hi, init = p[param]
            rng = hi - lo
            # 每个参数不同的波动特性
            seed = hash(pid + param) % 1000
            configs[pid][param] = {
                "period": 8 + (seed % 20),        # 周期 8-28秒
                "amplitude": 0.08 + (seed % 7) * 0.02,  # 振幅 8%-20% 范围
                "phase": (seed % 100) * 0.1,       # 相位偏移
                "noise": 0.01 + (seed % 5) * 0.005, # 噪声 1%-3%
                "drift": 0,                        # 漂移（P007/P008 特殊处理）
            }
            # scene 特殊趋势
            scene = p["scene"]
            if scene == "deteriorating":
                if param in ("hr","rr","temp"):
                    configs[pid][param]["drift"] = 0.15 + (seed % 10) * 0.02  # 持续上升
                elif param in ("spo2","sbp","dbp"):
                    configs[pid][param]["drift"] = -0.1 - (seed % 10) * 0.02  # 持续下降
            elif scene == "severe":
                configs[pid][param]["amplitude"] = 0.15 + (seed % 5) * 0.03   # 高振幅
                configs[pid][param]["period"] = 5 + (seed % 10)               # 短周期（快速波动）
            elif scene == "hypertensive":
                if param in ("sbp","dbp"):
                    configs[pid][param]["amplitude"] = 0.12  # 血压大幅波动
            elif scene == "tachycardia":
                if param == "hr":
                    configs[pid][param]["period"] = 6  # 心率快速波动
                    configs[pid][param]["amplitude"] = 0.12
            elif scene == "hypoxic":
                if param == "spo2":
                    configs[pid][param]["period"] = 10
                    configs[pid][param]["amplitude"] = 0.15
            elif scene == "febrile":
                if param == "temp":
                    configs[pid][param]["period"] = 20
                    configs[pid][param]["amplitude"] = 0.08
    return configs

WAVE_CONFIGS = build_wave_configs()

def gen_vital(patient, param, prev, elapsed):
    """生成动态变化的体征值。"""
    lo, hi, init = patient[param]
    pid = patient["pid"]
    cfg = WAVE_CONFIGS[pid][param]
    val = prev if prev else init
    t = elapsed

    # 1. 正弦主波动（每个参数不同周期/振幅/相位）
    wave = math.sin(t * 2 * math.pi / cfg["period"] + cfg["phase"])
    wave *= (hi - lo) * cfg["amplitude"]

    # 2. 次级波动（叠加一个不同频率的波，增加复杂度）
    wave2 = math.sin(t * 2 * math.pi / (cfg["period"] * 0.6) + cfg["phase"] * 1.7)
    wave2 *= (hi - lo) * cfg["amplitude"] * 0.4

    # 3. 随机噪声
    noise = random.gauss(0, (hi - lo) * cfg["noise"])

    # 4. 长期漂移（趋势）
    drift = cfg["drift"] * (t / 10)  # 每10秒漂移一次

    val += wave + wave2 + noise + drift

    # 5. 场景特殊逻辑
    scene = patient["scene"]
    if scene == "healthy":
        # 健康患者偶尔有小波动（咳嗽、活动）
        if random.random() < 0.02:  # 2%概率
            val += random.choice([-5, 5]) if param == "hr" else 0
    elif scene == "severe" and param == "hr":
        # 重症患者心率忽快忽慢
        if random.random() < 0.03:
            val += random.choice([-15, 15])

    # 钳制
    return round(max(lo, min(hi, val)), 1)


async def device_loop(patient, device_type, interval, running_flag, param_list=None):
    host = os.environ.get("COLLECTOR_HOST", "127.0.0.1")
    port = int(os.environ.get("COLLECTOR_PORT", "9001"))
    pid_num = patient["pid"]
    device_id = f"{device_type[:3].lower()}_{pid_num}"
    if param_list is None:
        param_list = ["hr","sbp","dbp","spo2","rr"]

    for attempt in range(10):
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            auth = json.dumps({"type":"auth","deviceId":device_id,"deviceType":device_type,"patientId":pid_num,"secret":"sim_secret_2024"})
            w.write(struct.pack("!I", len(auth.encode())) + auth.encode())
            await asyncio.wait_for(w.drain(), timeout=3)
            hdr = await asyncio.wait_for(r.readexactly(4), timeout=5)
            plen = struct.unpack("!I", hdr)[0]
            await asyncio.wait_for(r.readexactly(plen), timeout=5)
            break
        except Exception:
            if attempt == 9:
                return
            await asyncio.sleep(1)

    seq = 0
    values = {}
    start_ts = time.time()

    try:
        while running_flag["running"]:
            seq += 1
            obs = []
            elapsed = time.time() - start_ts
            for p in param_list:
                v = gen_vital(patient, p, values.get(p), elapsed)
                values[p] = v
                obs.append({"code":CODES[p],"name":NAMES[p],"value":v,"unit":UNITS[p]})
            ts = time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + f"{int(time.time()*1000)%1000:03d}Z"
            msg = {"type":"vitals","deviceId":device_id,"deviceType":device_type,"patientId":pid_num,"timestamp":ts,"sequence":seq,"observations":obs}
            w.write(struct.pack("!I", len(json.dumps(msg).encode())) + json.dumps(msg).encode())
            await asyncio.sleep(interval/1000)
    except Exception:
        pass
    finally:
        try: w.close()
        except: pass


async def main():
    print("=" * 60)
    print("  8 患者临床模拟器 — 全动态数据")
    print("  每个患者体征独立波动 + 场景趋势")
    print("  按 Ctrl+C 停止")
    print("=" * 60)
    print()
    print("  患者波动特性:")
    for p in PATIENTS:
        cfg = WAVE_CONFIGS[p["pid"]]
        ex = list(cfg.items())[0][1]
        print(f"    {p['pid']} {p['scene']:<15s} 周期≈{ex['period']}s 振幅≈{ex['amplitude']*100:.0f}% 趋势={ex['drift']}")
    print()

    # 5 种设备类型：各管一种体征，频率不同
    DEVICE_TYPES = [
        ("HeartRate",      1000, ["hr"]),        # 1秒/次
        ("BloodPressure",  5000, ["sbp","dbp"]), # 5秒/次
        ("SpO2Monitor",    2000, ["spo2"]),      # 2秒/次
        ("Respiratory",    3000, ["rr"]),        # 3秒/次
        ("Temperature",   30000, ["temp"]),      # 30秒/次
    ]

    running_flag = {"running": True}
    tasks = []
    for p in PATIENTS:
        for dev_name, interval, params in DEVICE_TYPES:
            tasks.append(device_loop(p, dev_name, interval, running_flag, params))

    print(f"  共 {len(PATIENTS)} 患者 × 5 设备 = {len(tasks)} 连接，数据持续发送中...\n")

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        running_flag["running"] = False
        print("\n  已停止")

    print("\n  仪表盘: http://localhost:8000/test")
    print()

if __name__ == "__main__":
    asyncio.run(main())
