#!/usr/bin/env python3
"""
50 患者临床数据模拟器 — 从主机直接运行（已验证 TCP 连接正常工作）。
协议: TCP + Length(4B BE) + JSON → localhost:9001
"""

import asyncio
import json
import os
import random
import struct
import sys
import time

random.seed(42)

# ── 8 种临床场景 ──────────────────────────────────
PATIENT_PROFILES = {
    "healthy":        {"hr":(60,100,72),"sbp":(100,140,120),"dbp":(60,90,80),"spo2":(95,100,98),"rr":(12,20,16),"temp":(36.0,37.5,36.8),"cnt":30},
    "hypertensive":   {"hr":(60,100,75),"sbp":(150,200,165),"dbp":(90,120,100),"spo2":(93,98,96),"rr":(14,22,18),"temp":(36.0,37.5,36.6),"cnt":4},
    "tachycardia":    {"hr":(110,140,120),"sbp":(100,130,115),"dbp":(60,85,75),"spo2":(94,99,97),"rr":(18,26,22),"temp":(36.5,38.0,37.2),"cnt":3},
    "bradycardia":    {"hr":(40,55,48),"sbp":(90,120,105),"dbp":(55,75,65),"spo2":(93,98,96),"rr":(10,16,13),"temp":(35.8,37.0,36.4),"cnt":2},
    "hypoxic":        {"hr":(85,120,100),"sbp":(100,130,115),"dbp":(60,85,72),"spo2":(85,92,90),"rr":(20,30,24),"temp":(36.5,38.0,37.0),"cnt":3},
    "febrile":        {"hr":(90,120,105),"sbp":(100,130,115),"dbp":(60,85,72),"spo2":(94,99,97),"rr":(18,26,22),"temp":(38.5,40.0,39.2),"cnt":3},
    "deteriorating":  {"hr":(75,140,80),"sbp":(100,140,120),"dbp":(60,90,80),"spo2":(98,85,97),"rr":(14,30,16),"temp":(36.5,39.5,37.0),"cnt":3},
    "severe":         {"hr":(120,160,135),"sbp":(80,100,90),"dbp":(50,65,58),"spo2":(80,90,86),"rr":(28,40,32),"temp":(38.0,40.0,38.8),"cnt":2},
}

def build_profiles():
    profiles = []
    pid = 1
    for scene_name, scene in PATIENT_PROFILES.items():
        for _ in range(scene["cnt"]):
            profiles.append({"patient_id": f"P{pid:04d}", "bed_id": f"ICU-{100+pid}", "scene": scene_name, "params": scene})
            pid += 1
    return profiles[:50]

def gen_vital(params, param, prev, elapsed, scene):
    lo, hi, init = params[param]
    val = (prev if prev else init) + random.gauss(0, (hi-lo)*0.02)
    if scene == "deteriorating":
        d = elapsed / 30.0
        if param == "hr":    val += d * 50
        if param == "spo2":  val -= d * 12
        if param == "temp":  val += d * 2.5
        if param == "rr":    val += d * 15
        if param in ("sbp","dbp"): val -= d * 20
    return round(max(lo, min(hi, val)), 1)

PATIENT_UNITS = {"hr":"/min","rr":"/min","sbp":"mmHg","dbp":"mmHg","spo2":"%","temp":"°C"}
PATIENT_CODES = {"hr":"8867-4","sbp":"8480-6","dbp":"8462-4","spo2":"2708-6","rr":"9279-1","temp":"8310-5"}
PATIENT_NAMES = {"hr":"Heart Rate","sbp":"Systolic BP","dbp":"Diastolic BP","spo2":"Oxygen Saturation","rr":"Respiratory Rate","temp":"Body Temperature"}
MONITOR_PARAMS = ["hr","sbp","dbp","spo2","rr"]
TEMP_PARAMS = ["temp"]

async def device_loop(pid, scene, params, device_type, interval, duration, sem):
    """单个设备循环。"""
    host = "127.0.0.1"
    port = 9001
    device_id = f"{'mon' if device_type=='Monitor' else 'tmp'}_{pid}"

    async with sem:
        for attempt in range(5):
            try:
                r, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
                auth = json.dumps({"type":"auth","deviceId":device_id,"deviceType":device_type,"patientId":f"P{pid}","secret":"sim_secret_2024"})
                w.write(struct.pack("!I", len(auth.encode())) + auth.encode())
                await asyncio.wait_for(w.drain(), timeout=3)
                hdr = await asyncio.wait_for(r.readexactly(4), timeout=5)
                plen = struct.unpack("!I", hdr)[0]
                await asyncio.wait_for(r.readexactly(plen), timeout=5)
                break
            except Exception as e:
                if attempt == 4:
                    return
                await asyncio.sleep(0.5)

    seq = 0
    values = {}
    start = time.time()
    param_list = MONITOR_PARAMS if device_type == "Monitor" else TEMP_PARAMS

    try:
        end = time.time() + duration
        while time.time() < end:
            seq += 1
            obs = []
            for p in param_list:
                v = gen_vital(params, p, values.get(p), time.time()-start, scene)
                values[p] = v
                obs.append({"code":PATIENT_CODES[p],"name":PATIENT_NAMES[p],"value":v,"unit":PATIENT_UNITS[p]})

            ts = time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + f"{int(time.time()*1000)%1000:03d}Z"
            msg = {"type":"vitals","deviceId":device_id,"deviceType":device_type,"patientId":f"P{pid}","timestamp":ts,"sequence":seq,"observations":obs}
            w.write(struct.pack("!I", len(j:=json.dumps(msg).encode())) + j)
            await asyncio.sleep(interval/1000)
    except:
        pass
    finally:
        try: w.close()
        except: pass

async def main():
    duration = int(os.environ.get("SIM_DURATION_SEC", "35"))
    profiles = build_profiles()

    scenes = {}
    for p in profiles:
        scenes[p["scene"]] = scenes.get(p["scene"], 0) + 1

    print("=" * 70)
    print("  🏥 医院模拟器 - 50 患者 × 8 场景")
    print(f"  场景: {scenes}")
    print(f"  目标: localhost:9001")
    print(f"  持续: {duration} 秒")
    print("=" * 70)

    # 连接数限制：最多同时 15 个连接
    sem = asyncio.Semaphore(15)
    tasks = []

    for p in profiles:
        pid_num = int(p["patient_id"][1:])
        tasks.append(device_loop(pid_num, p["scene"], p["params"], "Monitor", 1000, duration, sem))
        tasks.append(device_loop(pid_num, p["scene"], p["params"], "TempSensor", 30000, duration, sem))

    print(f"\n  启动 {len(profiles)} 患者 × 2 设备 = {len(tasks)} 连接\n")
    start = time.time()
    await asyncio.gather(*tasks)
    elapsed = time.time() - start
    print(f"\n  ✅ 完成! {elapsed:.0f} 秒")
    print(f"  查看: http://localhost:8000/docs")
    print()

if __name__ == "__main__":
    asyncio.run(main())
